#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

$:.unshift File.join(File.dirname(__FILE__), '..')
$:.unshift File.join(File.dirname(__FILE__), '..', 'lib')
$LOAD_PATH.unshift(File.expand_path("../../../", __FILE__))
$LOAD_PATH.unshift(File.expand_path("../../lib", __FILE__))

require "rubygems"
require "rspec"
require 'bundler/setup'
require 'vcap_services_base'
require "socket"
require "timeout"

require "hadoop/lib/hadoop_service/hadoop_node"
require "hadoop/lib/hadoop_service/hadoop_conf"


include VCAP::Services::Hadoop

def is_port_open?(host, port)
  begin
    Timeout::timeout(1) do
      begin
        TCPSocket.new(host, port).close
        true
      rescue Errno::ECONNREFUSED, Errno::EHOSTUNREACH => e
        false
      end
    end
  rescue Timeout::Error => e
    false
  end
end

def get_logger
  logger = Logger.new(STDOUT)
  logger.level = Logger::ERROR
  logger
end

def parse_property(hash, key, type, options = {})
  obj = hash[key]
  if obj.nil?
    raise "Missing required option: #{key}" unless options[:optional]
    nil
  elsif type == Range
    raise "Invalid Range object: #{obj}" unless obj.kind_of?(Hash)
    first, last = obj["first"], obj["last"]
    raise "Invalid Range object: #{obj}" unless first.kind_of?(Integer) and last.kind_of?(Integer)
    Range.new(first, last)
  else
    raise "Invalid #{type} object: #{obj}" unless obj.kind_of?(type)
    obj
  end
end

def config_base_dir
  File.join(File.dirname(__FILE__), '..', 'config')
end

def resources_base_dir
  File.join(File.dirname(__FILE__), '..', 'resources')
end



def get_node_config
  config_file = File.join(config_base_dir, "hadoop_node.yml")
  config = YAML.load_file(config_file)
  options = {
    :capacity => parse_property(config, "capacity", Integer),
    :logger => get_logger,
    :node_id => parse_property(config, "node_id", String),
    :mbus => parse_property(config, "mbus", String),
    :base_dir => '/tmp/hadoop/instances',
    :local_db => 'sqlite3:/tmp/hadoop/hadoop_node.db',
    :hostname => parse_property(config, "hostname", String),
    :port_range => parse_property(config, "port_range", Range),
    :hadoop => parse_property(config, "hadoop", Array),
  }
  options
end
