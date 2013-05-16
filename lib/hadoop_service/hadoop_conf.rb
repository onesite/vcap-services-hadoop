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
require "erb"
require "ostruct"
require 'zip/zip'

class HadoopConfigGenerator

  class ERBConfOMatic < OpenStruct
    def self.render_from_hash(template, values={})
      ERBConfOMatic.new(values).render(template)
    end

    def render(template)
      ERB.new(template).result(binding)
    end
  end

  def initialize(base_dir, instance)
    @base_dir = base_dir
    @instance = instance
    @data_dir = File.join(@base_dir, "data")
    @config_dir = File.join(@base_dir, "conf")
  end

  def generate_files
    FileUtils.mkdir_p @config_dir unless File.directory?(@config_dir)

    unzip_default_config("hadoop-default-config.zip", @config_dir)

    write_config(@config_dir, "core-site.xml", {
      "hostname" => @instance.hostname,
      "data_dir" => @data_dir,
      "fs_default_name_port" => @instance.fs_default_name_port
    })

    write_config(@config_dir, "hdfs-site.xml", {
      "hostname" => @instance.hostname,
      "data_dir" => @data_dir,
      "dfs_http_address_port" => @instance.dfs_http_address_port,
      "dfs_datanode_address_port" => @instance.dfs_datanode_address_port,
      "dfs_datanode_ipc_address_port" => @instance.dfs_datanode_ipc_address_port,
      "dfs_datanode_http_address_port" => @instance.dfs_datanode_http_address_port
    })

    write_config(@config_dir, "mapred-site.xml",{
      "hostname" => @instance.hostname,
      "data_dir" => @data_dir,
      "mapred_job_tracker_port" => @instance.mapred_job_tracker_port,
      "mapred_job_tracker_http_address_port" => @instance.mapred_job_tracker_http_address_port,
      "mapred_task_tracker_http_address_port" => @instance.mapred_task_tracker_http_address_port
    })

    write_config(@config_dir, "masters",{
        "hostname" => @instance.hostname,
    })

    write_config(@config_dir, "slaves",{
        "hostname" => @instance.hostname,
    })

    write_config(@config_dir, "hadoop-env.sh", {
        "base_dir" => @base_dir
    }, 0755)

    write_config(@config_dir, "log4j.properties", {
        "base_dir" => @base_dir
    })

  end

private

  def unzip_default_config (file, destination)
    archive = File.expand_path("../../../resources/#{file}", __FILE__)

    Zip::ZipFile.open(archive) do |zip_file|
      zip_file.each do |file|
        path = File.join(destination, file.name)
        FileUtils.mkdir_p(File.dirname(path))
        zip_file.extract(file, path) unless File.exist?(path)
      end
    end
  end

  def write_config(dir, templateName, values, file_perms=0644)
    template = File.read(File.expand_path("../../../resources/#{templateName}.erb", __FILE__))
    config = ERBConfOMatic::render_from_hash(template, values)
    config_file = File.join(dir, templateName)
    File.open(config_file, "w") { |f| f.write(config) }
    File.chmod(file_perms, config_file)
  end

end
