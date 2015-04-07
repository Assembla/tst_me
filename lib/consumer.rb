require "configuration"
require "mq_connection"

class Consumer

  def self.run(opts)
    begin
      processor = opts[:processor]
      queue_name = opts[:queue_name]
      routing_key = opts[:routing_key]
      require "processors/#{processor}"

      mq_connection = MQConnection.new
      channel = mq_connection.channel
      loop do
        begin
          consumer = processor.camelize.constantize.new
          mq_connection.subscribe(queue_name, {:manual_ack => true, :routing_key => routing_key}) do |delivery_info, payload|
            payload = HashWithIndifferentAccess.new(JSON.parse(payload))
            consumer.metadata = delivery_info
            consumer.on_message(payload)
            channel.acknowledge(delivery_info.delivery_tag)
          end
        rescue => ex
          puts "\nError:\n #{ex.backtrace.join("\n")}"
          sleep(5)
        end
      end
    rescue Interrupt
      puts "Consumer stopped."
      exit(0)
    ensure
      mq_connection.connection.close if mq_connection && mq_connection.connection
    end
  end
end
