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

$:.unshift(File.dirname(__FILE__))
require 'spec_helper'

describe "Hadoop configuration generator" do

  before :each do
    @test_dir = "/tmp/vcap-hadoop-test"

    @service = VCAP::Services::Hadoop::Node::ProvisionedService.new(
        :name => "hadoop-12345",
        :hostname => "hadoop-01",
        :namenode_pid => 1234,
        :datanode_pid => 1235,
        :jobtracker_pid => 1236,
        :tasktracker_pid => 1237,
        :username => "user1",
        :password => "password1",
        :dfs_http_address_port => 5001,
        :dfs_datanode_address_port => 5002,
        :dfs_datanode_ipc_address_port => 5003,
        :dfs_datanode_http_address_port => 5004,
        :fs_default_name_port => 5005,
        :mapred_job_tracker_port => 5006,
        :mapred_job_tracker_http_address_port => 5007,
        :mapred_task_tracker_http_address_port => 5008
    )

    @base_dir = File.join(@test_dir, @service.name)
    @config_dir = File.join(@base_dir, "conf")

    @configurator = HadoopConfigGenerator.new(@base_dir, @service)
  end

  after :each do
    FileUtils.rm_rf(@test_dir)
  end

  it "should create the expected conf dir" do
    EM.run do
      @configurator.should be_instance_of HadoopConfigGenerator
      @configurator.generate_files
      File.directory?(@config_dir).should be_true

      EM.stop
    end
  end

  it "should create the configuration files" do
    EM.run do
      @configurator.should be_instance_of HadoopConfigGenerator
      @configurator.generate_files

      conf_files = Dir.chdir(resources_base_dir) { Dir.glob("*.erb") }

      conf_files.each do |config_file|
        File.exist?(File.join(@config_dir, config_file.gsub(/\.erb$/, ''))).should be_true
      end

      EM.stop
    end
  end

end