class BoxApiConstants
  BOX_ITEM_FIELDS = %w(type
      id
      sequence_id
      etag
      name
      created_at
      modified_at
      description
      size
      path_collection
      created_by
      modified_by
      trashed_at
      purged_at
      content_created_at
      content_modified_at
      owned_by
      shared_link
      parent
      item_status
      sha1
      version_number
      comment_count
      folder_upload_email
      item_collection
    )

  FILE_STRUCTURE_EVENTS = %w(
      ITEM_CREATE
      ITEM_UPLOAD
      ITEM_DOWNLOAD
      ITEM_PREVIEW
      ITEM_MOVE
      ITEM_COPY
      ITEM_TRASH
      ITEM_UNDELETE_VIA_TRASH
      ITEM_RENAME
      ITEM_SHARED_CREATE
      ITEM_SHARED_UNSHARE
      ITEM_SHARED
    )

  OTHER_BOX_EVENTS = %w(
      COLLAB_ADD_COLLABORATOR
      COLLAB_ROLE_CHANGE
      COLLAB_INVITE_COLLABORATOR
      COLLAB_REMOVE_COLLABORATOR
      TASK_ASSIGNMENT_CREATE
      COMMENT_CREATE
    )
end
