require "rubygems"
require "bundler/setup"
Bundler.require(:default)

require 'active_support/core_ext/hash'
require 'active_support/inflections'
require 'logger'
require 'json'
require "erb"
require "box_api_constants"
require "processors/base_processor"

def load_config(path)
  configatron.configure_from_hash(Yamler.load(path))
  configatron.hostname = Socket.gethostname
end

load_config(File.join("config", "settings.yml"))

local_cfg = "config/settings_local.yml"
load_config(local_cfg) if File.exists?(local_cfg)
