# -*- encoding: utf-8 -*-

class BoxRemoteSyncProcessor < BaseProcessor

  def on_message(event)
    logger.debug("#{self.class.name} received: #{event.inspect}")
    @event = event

    begin
      case metadata.routing_key
        when "box_connector.sync.full.requested"
          do_remote_full_sync_request(event)
        when "box_connector.sync.delta.requested"
          do_remote_delta_sync_request(event)
        else
          logger.info("Unknown routing key received: #{metadata.routing_key}")
      end
    rescue RubyBox::AuthError => ex
      # enqueue token refresh event
      logger.info("Exception: #{ex}")
      logger.info(ex.backtrace.join("\n"))
      invalidate_box_credentials
      resubmit_event(event, metadata.routing_key)
    end
  end

  private

  def do_remote_full_sync_request(event)
    connector_path = event[:connector][:path]
    result = []
    if event[:connector] && event[:connector][:external_item_id]
      item_id = event[:connector][:external_item_id]
      walk(connector_path, item_id, result)
    end

    event[:connector][:cursor] = Time.now.to_i * 1000 # box cursor is represented in milliseconds.
    event[:entries] = result
    mq_connection.publish(event, {:routing_key => "box_connector.sync.full.succeed"}) if result.size > 0
  end

  def do_remote_delta_sync_request(event)
    cursor = event[:connector][:cursor]
    result = []

    events_response = client.event_response(cursor.to_i, :changes)
    events = get_connector_related_events(events_response.events, event[:connector][:external_ids].to_a, event[:connector][:path])
    sorted_events = compress_events(events)
    external_event_ids = event[:connector][:external_event_ids]
    event[:connector][:external_event_ids] = []
    sorted_events.each do |item_id, last_event|
      if last_event && (external_event_ids.blank? || !external_event_ids.include?(last_event.event_id))
        node = get_node_metadata(event, last_event)
        result << node if node.present?
        event[:connector][:external_event_ids] << last_event.event_id
      end
    end

    event[:connector][:cursor] = events_response.next_stream_position
    event[:entries] = result.flatten

    # todo: treat this events in some other consumer
    # if BoxApi::OTHER_BOX_EVENTS.include?(event_type)
    #   do_event_tracking(last_event)
    # end

    mq_connection.publish(event, {:routing_key => "box_connector.sync.delta.succeed"})
  end

  def get_connector_related_events(events, external_ids, connector_path)
    events.select do |event|
      item_full_path = item_full_path(event.source)
      BoxApiConstants::FILE_STRUCTURE_EVENTS.include?(event.event_type) &&
        (external_ids.include?(event.source.id) || external_ids.include?(event.source.try(:parent).try(:id)) || item_full_path.include?(File.join(connector_path, "")))
    end
  end

  def compress_events(events)
    grouped_events = events.group_by{|ev| ev.source.id}
    grouped_events.each do |item_id, item_events|
      grouped_events[item_id] = if item_events.size >= 2
          item_events.max{|a,b| a.created_at <=> b.created_at}
        else
          item_events.first
        end
    end
  end

  def get_node_metadata(event, box_event)
    begin
      connector_path = event[:connector][:path]
      event_source = box_event.source
      full_path = clean_path(connector_path, event_source)
      case event_source.type
      when "file"
        event_type = box_event.event_type == "ITEM_TRASH" ? "/trash" : ""
        file_meta_url = "#{RubyBox::API_URL}/files/#{event_source.id}#{event_type}?fields=#{BoxApiConstants::BOX_ITEM_FIELDS.map(&:to_s).join(',')}"
        file_meta = box_session.get(file_meta_url)
        file_meta = RubyBox::Item.new(box_session, file_meta)
        file_node(full_path, file_meta, file_meta.parent.id) unless full_path.blank?
      when "folder"
        result = []
        result << folder_node(full_path, event_source, event_source.parent.id) unless full_path.blank?
        if box_event.event_type == "ITEM_COPY"
          walk(connector_path, event_source.id, result)
        end
        result
      else
        nil
      end
    rescue RubyBox::ObjectNotFound => ex
      # files from delete folders do not appear in the trash so those cannot be found.
      logger.info("event type: #{box_event.event_type}, source type: #{event_source.type}. Could not find item with external id: #{event_source.id}")
      logger.debug(ex)
      logger.debug(ex.backtrace.join("\n"))
      nil
    end
  end

  def process_file_version(file_item)
    versions_url = "#{RubyBox::API_URL}/files/#{file_item.id}/versions"
    versions = box_session.get(versions_url)
  end

  def walk(connector_path, parent_id, result)
    folder = client.folder_by_id(parent_id)
    folder.items(100, 0, BoxApiConstants::BOX_ITEM_FIELDS).each do |item|
      full_path = clean_path(connector_path, item)

      if item.type == "folder"
        result << folder_node(full_path, item, parent_id)
        walk(connector_path, item.id, result)
      else
        result << file_node(full_path, item, parent_id)
      end
    end
  end

  def clean_path(connector_path, item)
    regexp = /(#{File.join(connector_path, "")})/i
    full_path = item_full_path(item)
    full_path.gsub(regexp, "")
  end

  def item_full_path(item)
    File.join("", item.path_collection.entries.collect { |x| x["name"] }, item.name) if item.path_collection.present?
  end

  def file_node(full_path, item, parent_id)
    {
      external_id: item.id,
      external_parent_id: parent_id,
      name: item.name,
      full_path: full_path,
      is_dir: false,
      size: item.size,
      revision: item.version_number,
      sha_id: item.sha1,
      created_at: item.created_at,
      updated_at: item.modified_at,
      deleted_at: item.trashed_at || item.purged_at,
      created_by: {id: item.created_by.id, name: item.created_by.name, login: item.created_by.login},
      updated_by: {id: item.modified_by.id, name: item.modified_by.name, login: item.modified_by.login},
    }
  end

  def folder_node(full_path, item, parent_id)
    {
      external_id: item.id,
      external_parent_id: parent_id,
      name: item.name,
      full_path: full_path,
      is_dir: true,
      size: item.size,
      created_at: item.created_at,
      updated_at: item.modified_at,
      deleted_at: item.trashed_at || item.purged_at,
      created_by: {id: item.created_by.id, name: item.created_by.name, login: item.created_by.login},
      updated_by: {id: item.modified_by.id, name: item.modified_by.name, login: item.modified_by.login},
    }
  end

  def resubmit_event(event, routing_key)
    event[:errors] = "refresh_token"
    mq_connection.publish(event, {:routing_key => routing_key.gsub("requested", "oauth_failed")})
  end

  def invalidate_box_credentials
    @client = nil
    @box_session = nil
  end

  def mq_connection
    @connection ||= MQConnection.new
  end

  def client
    @client ||= RubyBox::Client.new(box_session)
  end

  def box_session
    @box_session ||= RubyBox::Session.new({client_id: configatron.box_connector.client_id,
                                        client_secret: configatron.box_connector.client_secret, access_token: @event[:oauth_token]})
  end
end
