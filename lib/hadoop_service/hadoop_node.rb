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

require "fileutils"
require "logger"
require "datamapper"
require "uuidtools"
require "set"
require 'vcap/common'

module VCAP
  module Services
    module Hadoop
      class Node < VCAP::Services::Base::Node
      end
    end
  end
end

require "hadoop_service/common"
require "hadoop_service/hadoop_conf"

class VCAP::Services::Hadoop::Node

  include VCAP::Services::Hadoop::Common

  class ProvisionedService
    include DataMapper::Resource
    property :name,         String,       :key => true
    property :hostname,     String
    property :username,     String
    property :password,     String

    property :namenode_pid,          Integer
    property :datanode_pid,          Integer
    property :jobtracker_pid,        Integer
    property :tasktracker_pid,       Integer

    property :dfs_http_address_port,                   Integer
    property :dfs_datanode_address_port,               Integer
    property :dfs_datanode_ipc_address_port,           Integer
    property :dfs_datanode_http_address_port,          Integer
    property :fs_default_name_port,                    Integer
    property :mapred_job_tracker_port,                 Integer
    property :mapred_job_tracker_http_address_port,    Integer
    property :mapred_task_tracker_http_address_port,   Integer

    def listening?
      begin
        TCPSocket.open('localhost', port).close
        return true
      rescue => e
        return false
      end
    end

    def running?(pid)
      VCAP.process_running? pid
    end

    def kill(sig=:SIGTERM)
      Process.kill(sig, namenode_pid) if running?(namenode_pid)
      Process.kill(sig, datanode_pid) if running?(datanode_pid)
      Process.kill(sig, jobtracker_pid) if running?(jobtracker_pid)
      Process.kill(sig, tasktracker_pid) if running?(tasktracker_pid)
    end
  end

  def initialize(options)
    super(options)
    @base_dir = options[:base_dir]
    FileUtils.mkdir_p(@base_dir) unless File.directory?(@base_dir)

    @hostname = options[:hostname]

    @hadoop_config = options[:hadoop_config]
    @hadoop_runtime_path = @hadoop_config["runtime_path"]

    @hadoop_instance_count = 0
    @hadoop_instance_limit = @hadoop_config["instance_limit"]

    DataMapper.setup(:default, options[:local_db])
    DataMapper::auto_upgrade!

    @free_ports = Set.new
    options[:port_range].each {|port| @free_ports << port}
    @mutex = Mutex.new
    @supported_versions = options[:supported_versions]
  end

  def fetch_port(port=nil)
    @mutex.synchronize do
      port ||= @free_ports.first
      raise "port #{port} is already taken!" unless @free_ports.include?(port)
      @free_ports.delete(port)
      port
    end
  end

  def return_port(port)
    @mutex.synchronize do
      @free_ports << port
    end
  end

  def delete_port(port)
    @mutex.synchronize do
      @free_ports.delete(port)
    end
  end

  def pre_send_announcement
    super
    @capacity_lock.synchronize do
      ProvisionedService.all.each do |instance|
        @capacity -= capacity_unit
      end
    end
  end

  def shutdown
    super
    @logger.info("Shutting down instances..")
    ProvisionedService.all.each do |service|
      @logger.info("Shutting down #{service}")
      stop_service(service)
    end
  end

  def announcement
    @capacity_lock.synchronize do
      { :available_capacity => @capacity,
        :capacity_unit => capacity_unit }
    end
  end

  def provision(plan, credentials=nil, version=nil)
    @logger.debug("Hadoop Instance Limit: #{@hadoop_instance_limit} of #{@hadoop_instance_count}")

    if(@hadoop_instance_count < @hadoop_instance_limit)
      @hadoop_instance_count += 1
    else
      raise "Hadoop instance limit of #{@hadoop_instance_limit} has been reached for #{@hostname}"
    end

    provisioned_service = ProvisionedService.new
    if credentials
      provisioned_service.name = credentials["name"]
      provisioned_service.username = credentials["username"]
      provisioned_service.password = credentials["password"]
    else
      provisioned_service.name = "hadoop-#{UUIDTools::UUID.random_create.to_s}"
      provisioned_service.username = UUIDTools::UUID.random_create.to_s
      provisioned_service.password = UUIDTools::UUID.random_create.to_s
    end

    provisioned_service.hostname = "#{@hostname}"

    provisioned_service.dfs_http_address_port = fetch_port.to_i
    provisioned_service.dfs_datanode_address_port = fetch_port.to_i
    provisioned_service.dfs_datanode_ipc_address_port = fetch_port.to_i
    provisioned_service.dfs_datanode_http_address_port = fetch_port.to_i
    provisioned_service.fs_default_name_port = fetch_port.to_i
    provisioned_service.mapred_job_tracker_port = fetch_port.to_i
    provisioned_service.mapred_job_tracker_http_address_port = fetch_port.to_i
    provisioned_service.mapred_task_tracker_http_address_port = fetch_port.to_i

    start_instance(provisioned_service)

    provisioned_service.namenode_pid = hadoop_get_pid_for_service_type(provisioned_service, "namenode")
    provisioned_service.datanode_pid = hadoop_get_pid_for_service_type(provisioned_service, "datanode")
    provisioned_service.jobtracker_pid = hadoop_get_pid_for_service_type(provisioned_service, "jobtracker")
    provisioned_service.tasktracker_pid = hadoop_get_pid_for_service_type(provisioned_service, "tasktracker")

    unless provisioned_service.save
      @logger.error("Unable to save #{provisioned_service.inspect}")
      cleanup_service(provisioned_service)
      raise "Could not save entry: #{provisioned_service.errors.pretty_inspect}"
    end

    response = gen_credentials(provisioned_service)
    @logger.debug("response: #{response}")
    return response
  rescue => e
    @logger.warn(e)
  end

  def unprovision(name, credentials)
    provisioned_service = ProvisionedService.get(name)
    raise "Could not find service: #{name}" if provisioned_service.nil?

    cleanup_service(provisioned_service)
    @logger.debug("Unprovisioned Hadoop service #{name}")
    @hadoop_instance_count -= 1
  end

  def cleanup_service(service)
    @logger.debug("Killing #{service.name}")

    stop_service(service)
    raise "Could not cleanup service: #{service.errors.pretty_inspect}" unless service.new? || service.destroy

    service.kill
    service_dir = File.join(@base_dir, service.name)

    EM.defer do
      FileUtils.rm_rf(service_dir)
    end

    return_port(service.dfs_http_address_port)
    return_port(service.dfs_datanode_address_port)
    return_port(service.dfs_datanode_ipc_address_port)
    return_port(service.dfs_datanode_http_address_port)
    return_port(service.fs_default_name_port)
    return_port(service.mapred_job_tracker_port)
    return_port(service.mapred_job_tracker_http_address_port)
    return_port(service.mapred_task_tracker_http_address_port)

    true
  rescue => e
    @logger.warn(e)
  end

  def stop_service(service)
    @logger.info("Stopping #{service.name}")

    begin
      service_dir = File.join(@base_dir, service.name)
      config_dir = File.join(service_dir, "conf")
      cmd = File.join(@hadoop_runtime_path, "bin", "hadoop-daemon.sh")

      status = hadoop_run_sys_call(service, "#{cmd} --config #{config_dir} stop namenode")
      @logger.send(status.success? ? :debug : :error, "Service #{service.name} namenode stopped")
      status = hadoop_run_sys_call(service, "#{cmd} --config #{config_dir} stop datanode")
      @logger.send(status.success? ? :debug : :error, "Service #{service.name} datanode stopped")
      status = hadoop_run_sys_call(service, "#{cmd} --config #{config_dir} stop jobtracker")
      @logger.send(status.success? ? :debug : :error, "Service #{service.name} jobtracker stopped")
      status = hadoop_run_sys_call(service, "#{cmd} --config #{config_dir} stop tasktracker")
      @logger.send(status.success? ? :debug : :error, "Service #{service.name} tasktracker stopped")
    rescue => e
      @logger.error("Error stopping service #{service.name}: #{e}")
    end

    service.kill(:SIGTERM)
  end

  def bind(name, bind_opts, credentials=nil)
    @logger.debug("Bind request: name=#{name}, bind_opts=#{bind_opts}")

    provisioned_service = ProvisionedService.get(name)
    raise "Could not find service: #{name}" if provisioned_service.nil?

    response = gen_credentials(provisioned_service)
    @logger.debug("response: #{response}")
    response
  end

  def unbind(credentials)
    @logger.debug("Unbind request: credentials=#{credentials}")

    name = credentials['name']
    provisioned_service = ProvisionedService.get(name)
    raise "Could not find service: #{name}" if provisioned_service.nil?
    # Hadoop has nothing to unbind
    @logger.debug("Successfully unbound #{credentials}")
    true
  rescue => e
    @logger.warn(e)
    nil
  end

  def start_instance(service)
    @logger.debug("Starting: #{service.name}")

    service_dir = File.join(@base_dir, service.name)
    config_dir = File.join(service_dir, "conf")

    setup_server(service_dir, service) unless File.directory?(config_dir)

    @logger.info("Starting #{service.name}")
    begin
      cmd = File.join(@hadoop_runtime_path, "bin", "hadoop-daemon.sh")
      config_dir = File.join(service_dir, "conf")

      status = hadoop_run_sys_call(service, "#{cmd} --config #{config_dir} start namenode")
      @logger.send(status.success? ? :debug : :error, "Service #{service.name} namenode started with pid #{status.pid}")
      status = hadoop_run_sys_call(service, "#{cmd} --config #{config_dir} start datanode")
      @logger.send(status.success? ? :debug : :error, "Service #{service.name} datanode started with pid #{status.pid}")
      status = hadoop_run_sys_call(service, "#{cmd} --config #{config_dir} start jobtracker")
      @logger.send(status.success? ? :debug : :error, "Service #{service.name} jobtracker started with pid #{status.pid}")
      status = hadoop_run_sys_call(service, "#{cmd} --config #{config_dir} start tasktracker")
      @logger.send(status.success? ? :debug : :error, "Service #{service.name} tasktracker started with pid #{status.pid}")
    rescue => e
      @logger.error "Error starting service #{service.name}: #{e}"
    end
  end

  def setup_server(service_dir, service)
    @logger.info("Installing Hadoop in #{service_dir}")

    # Setup top level folders for hadoop service
    %w{ conf logs data run }.each do |dir|
      FileUtils.mkdir_p(File.join(service_dir, dir))
    end

    # Setup all other necessary directories for hadoop data usage
    %w{ data name namesecondary }.each do |dir|
      FileUtils.mkdir_p(File.join(service_dir, "data", "dfs", dir))
    end

    %w{ local system staging temp }.each do |dir|
      FileUtils.mkdir_p(File.join(service_dir, "data", "mapred", dir))
    end

    HadoopConfigGenerator.new(service_dir, service).generate_files

    begin
      cmd = File.join(@hadoop_runtime_path, "bin", "hadoop")
      hadoop_run_sys_call(service, "yes Y | #{cmd} namenode -format")
    rescue => e
      @logger.error "Error formatting hdfs for service #{service.name} with #{cmd}: #{e}"
    end
  end

  def gen_credentials(service)
    raise "Could not access provisioned service" unless service
    credentials = {
        "name"     => "#{service.name}",
        "hostname"     => "#{@hostname}",
        "username" => "#{service.username}",
        "password" => "#{service.password}",
        "dfs_http_address_port" => service.dfs_http_address_port,
        "dfs_datanode_address_port" => service.dfs_datanode_address_port,
        "dfs_datanode_ipc_address_port" => service.dfs_datanode_ipc_address_port,
        "dfs_datanode_http_address_port" => service.dfs_datanode_http_address_port,
        "fs_default_name_port" => service.fs_default_name_port,
        "mapred_job_tracker_port" => service.mapred_job_tracker_port,
        "mapred_job_tracker_http_address_port" => service.mapred_job_tracker_http_address_port,
        "mapred_task_tracker_http_address_port" => service.mapred_task_tracker_http_address_port
    }
    credentials
  end

  def hadoop_run_sys_call(service, cmd)
    service_dir = File.join(@base_dir, service.name)
    system({
               "HADOOP_PREFIX" => @hadoop_runtime_path,
               "HADOOP_CONF_DIR" => File.join(service_dir, "conf"),
               "HADOOP_LOG_DIR" => File.join(service_dir, "logs"),
               "HADOOP_PID_DIR" => File.join(service_dir, "run")
           },
           cmd
    )
    status = $?
    return status
  end

  def hadoop_get_pid_for_service_type(service, type)
    user = ENV['USER']
    pidfile =  File.join(@base_dir, service.name, "run", "hadoop-#{user}-#{type}.pid")
    pid = `[ -f #{pidfile} ] && cat #{pidfile}`
    status = $?
    return status.pid.to_i
  end

end

