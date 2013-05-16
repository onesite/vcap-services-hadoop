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

def provision
  @default_plan = "free"
  @node.provision(@default_plan)
end

def unprovision
  @node.unprovision(@service.name) unless @service.nil?
end

describe "Hadoop node provisioner" do

  before :all do
    @test_dir = "/tmp/vcap-hadoop-test"

    @opts = get_node_config
    @opts.freeze
    @logger = @opts[:logger]

    EM.run do
      @node = Node.new(@opts)
      EM.add_timer(1) { EM.stop }
    end
  end

  before :each do
    @service = provision
  end

  after :each do
    FileUtils.rm_rf(@test_dir)
  end

  it "should start new Hadoop service instance" do
    begin
      `ps #{@service.nadnode_pid}`.include?("hadoop").should be_true
      `ps #{@service.datanode_pid}`.include?("hadoop").should be_true
      `ps #{@service.jobtracker_pid}`.include?("hadoop").should be_true
      `ps #{@service.tasktracker_pid}`.include?("hadoop").should be_true
    ensure
      @node.unprovision(@service.name)
    end
  end

  it "should delete the hadoop service directory on unprovision" do
    File.exist?(File.join(@test_dir, @service.name)).should be_true
    @node.unprovision(@service.name)
    File.exist?(File.join(@test_dir, @service.name)).should be_false
  end

  it "should return an error when unprovisioning a non-existent instance" do
    EM.run do
      e = nil
      begin
        @node.unprovision('not existent', [])
      rescue => e
      end
      e.should_not be_nil
      EM.stop
    end
  end

  it "should unprovision an existent instance" do
    EM.run do
      e = nil
      begin
        @node.unprovision(@service.name, [])
      rescue => e
      end
      e.should be_nil
      EM.stop
    end
  end

end