# encoding: utf-8
require "logstash/outputs/base"
require 'cassandra'
require 'json'
require_relative './tool_adaptive.rb'

# An mycrassandra output that does nothing.
class LogStash::Outputs::Mycrassandra < LogStash::Outputs::Base
  attr_accessor :result, :keyspace, :cluster, :session, :tool
  config_name "mycrassandra"
  config :cra_database, :validate => :string, :default => "zipkin2"
  # config :cra_table, :validate => :string, :default => "zipkin2"
  config :cra_name, :validate => :string, :default => "root"
  config :cra_pwd, :validate => :string, :default => "root123"
  config :cra_hosts, :validate => :array, :default => ['192.100.5.59']
  config :cqlFile_path, :validate => :string, :default => "cql.conf"

  @@calc_count = 0

  public

  def register
    @logger.info("register  beging===============================================")
       @cluster = Cassandra.cluster(username: cra_name,
                                 password: cra_pwd,
                                 hosts: cra_hosts)
    @keyspace = cra_database
    #  @session = @cluster.connect(@keyspace) # create session, optionally scoped to a keyspace, to execute queries
    @cluster.each_host do |host| # automatically discovers all peers
      @logger.info("Host #{host.ip}: id=#{host.id} datacenter=#{host.datacenter} rack=#{host.rack}")

    end
    connect_dra #创建session
    @tool = Tool_adaptive.new #创建工具
    @logger.info("register  end===============================================")
  end


  def connect_dra
    begin
      @session = @cluster.connect(@keyspace)
    rescue IOError => e
      @logger.info("ex===fromm  connect_dra  ,sleeping...#{e.message}")
      @cluster.each_host do |host| # automatically discovers all peers
        @logger.warn("Host #{host.ip}: id=#{host.id} datacenter=#{host.datacenter} rack=#{host.rack}")
      end
      sleep(2)
      retry
    end
  end

  public

  def receive(event)

    begin
      myobject = {}.merge(event.to_hash)

    rescue Exception => e
      @logger.warn("parse event ====>ex===>#{ e.message }")
      raise e
    end
    mess = myobject['message'].to_s

    logs = JSON.parse mess.gsub(/\\/, '')
    class_type = logs.class.to_s
    if "Array" == class_type

      for mydoc in logs
        #model1
        #cql_cra = parse_cql(mydoc)  ###计划使用自己定义的配置文件解析cql
        # 不用解析cql，但是使用人员比较难用
        #modle2
        insert_run(mydoc)
        @@calc_count += 1
      end
    else
      if "Hash" == class_type
        insert_run(logs)
      end

    end
    @logger.info("current calc_count:===>#{@@calc_count}")
    return "event  received"
  end

  def insert_run(mydoc)
    begin
      cql_str = File.read(cqlFile_path)
      cql_h = eval(cql_str)
      cql_h.each do |k, v|
        execute_cql(k, v)
      end
    rescue Exception => e
      @logger.warn("insert_run  method raise ex ====>#{e.message}")
      raise e
    end
  end

  def execute_cql(k, v)
    begin
      @session.execute(v)
    rescue Exception => e
      @logger.warn("ex====from execute_cql===next connect_dra====e===>#{e.message}===error insert ========#{k}=========>#{v}<======")
      connect_dra
    end
  end
end


# class LogStash::Outputs::Mycrassandra