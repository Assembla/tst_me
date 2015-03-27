# -*- encoding: utf-8 -*-

class BaseProcessor
  attr_accessor :metadata

  def initialize(metadata = nil)
    self.metadata = metadata
  end

  def logger
    @logger ||= Logger.new("log/poller_#{log_file_suffix}.log")
  end

  def log_file_suffix
    self.class.to_s.tableize.gsub('/', '-')
  end
end
