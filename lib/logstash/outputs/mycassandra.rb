# encoding: utf-8
require "logstash/outputs/base"
require 'cassandra'
require 'json'
require_relative './tool_adaptive.rb'
require_relative './interval.rb'

# An mycrassandra output that does nothing.
class LogStash::Outputs::Mycassandra < LogStash::Outputs::Base
  attr_accessor :result, :keyspace, :cluster, :session, :tool, :batch, :cql_h, :loggerNum
  config_name "mycassandra"
  config :cra_database, :validate => :string, :default => "zipkin2"
  config :cra_name, :validate => :string, :default => "xxx"
  config :cra_pwd, :validate => :string, :default => "xxx"
  config :cra_hosts, :validate => :array, :default => ['192.168.xx.xx']
  config :cqlFile_path, :validate => :string, :default => "cql.conf" #此文件默认设置会在logstash默认的安装目录下

  @@calc_count = 0

  public

  def register
    @logger.info("register  beging===========57-6====================================")
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

    @@interival = Thread.new do
      tool = Time_tools.new
      Thread.current
      tool.interval 'sec', 10, @@calc_count
    end



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
      #raise e
    end

    begin
      mess = myobject['message'].to_s

      logs = JSON.parse mess.gsub(/\\/, '')
      class_type = logs.class.to_s
      ##判断非空，验证logstash 会间断性的来调用输出插件，所以程序以下不该继续执行
      if logs == nil
        @logger.warn("end receive==logs is nil======calc_count:===>#{@@calc_count}=====mess:==>#{mess}==logs===>#{logs}==========")
        return "event  received"
      end
    rescue Exception => e
      @logger.warn("mess:====>#{mess}=====class_type:==>#{class_type}========JSON parse ====>ex===>#{ e.message }")
      #raise e
    end
    begin
      if "Array" == class_type

        for mydoc in logs
          #model1
          #cql_cra = parse_cql(mydoc)  ###计划使用自己定义的配置文件解析cql
          # 不用解析cql，但是使用人员比较难用
          #modle2
          insert_run(mydoc)
          @@calc_count += 1
        end
      elsif "Hash" == class_type
        insert_run(logs)
      else
        @logger.warn("===mycassandra  only access  Array or Hash======")
        #raise "mycassandra  only access  Array or Hash"
      end
    rescue Exception => e
      @logger.warn("error:=======>ex= from receive==>#{ e.message }")
    end

    # @logger.info("current calc_count:===>#{@@calc_count}")
    return "event  received"
  end

  def insert_run(mydoc)
    begin
      cql_str = File.read(cqlFile_path)
      @cql_h = eval(cql_str)
      insert_run_do(@cql_h)

    rescue Exception => e
      @logger.warn("insert_run  method raise ex ====>#{e.message}")
      #raise e
    end
  end

  def insert_run_do(cql_str)

    cql_h.each do |k, v|
      #execute_cql(k, v)#单条执行模式
      @batch = execute_cql_batch_add(k, v)
    end
    execute_cql_batch(@batch)
  end

  def execute_cql_batch(batch)
    begin
      @session.execute(batch)
    rescue Exception => e
      @logger.warn("ex====from execute_cql_batch===next connect_dra====e===>#{e.message}===error insert batch===@cql_h.values=====>#{@cql_h.values}=====")
      connect_dra
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

  def execute_cql_batch_add(k, v)
    begin
      batch = @session.batch do |batch|
        batch.add(v)
      end
      return batch
    rescue Exception => e
      @logger.warn("ex====from execute_cql===next connect_dra====e===>#{e.message}===error insert ========#{k}=========>#{v}<======")
      connect_dra
    end
  end
end


# class LogStash::Outputs::Mycrassandra