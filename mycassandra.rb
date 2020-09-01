# encoding: utf-8
require "logstash/outputs/base"
require 'cassandra'
require 'json'
require_relative './tool_adaptive.rb'
require_relative './interval.rb'

# An mycrassandra output that does nothing.
class LogStash::Outputs::Mycassandra < LogStash::Outputs::Base
  attr_accessor :result, :keyspace, :cluster, :session, :tool, :batch, :cql_h, :loggerNum, :count, :sum_duration
  config_name "mycassandra"
  config :cra_database, :validate => :string, :default => "zipkin2"
  config :cra_name, :validate => :string, :default => "root"
  config :cra_pwd, :validate => :string, :default => "xx"
  config :cra_hosts, :validate => :array, :default => ['192.xx.3.57']
  config :cqlFile_path, :validate => :string, :default => "cql.conf"

  @@calc_count = 0

  public

  def register
    @logger.info("register  beging===========57-20====================================")
    @cluster = Cassandra.cluster(username: cra_name,
                                 password: cra_pwd,
                                 hosts: cra_hosts)
    @keyspace = cra_database

    @cluster.each_host do |host| # automatically discovers all peers
      @logger.info("Host #{host.ip}: id=#{host.id} datacenter=#{host.datacenter} rack=#{host.rack}")
    end
    connect_dra #创建session
    @tool = Tool_adaptive.new #创建工具
    #待优化开启一个线程定时记录日志
    # @@interival = Thread.new do
    #   tool = Time_tools.new
    #   # Thread.current
    #   tool.interval 'sec', 10, @@calc_count
    # end


    @logger.info("register  end===============================================")
  end

## 建立session
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
##所有的event 都能转成hash
  def receive(event)
    begin
      myobject = {}.merge(event.to_hash)

    rescue Exception => e
      @logger.warn("parse event ====>ex===>#{ e.message }")
      #raise e
    end
    begin
      mess = myobject['message'].to_s
      # logs = JSON.parse mess.gsub(/\\/, '')
      logs = JSON.parse mess
      class_type = logs.class.to_s
      ##判断非空，验证logstash 会间断性的来调用输出插件，所以程序以下不该继续执行 但暂时不起作用日后优化
      if logs == nil || mess == nil || mess.empty?
        @logger.warn("end receive==mess or logs is nil======calc_count:===>#{@@calc_count}==messclass:==>#{mess.class}====mess:==>#{mess}======logsclass===>#{logs.class}===logs===>#{logs}==========")
        return "event  received"
      end
    rescue Exception => e
      @logger.warn("mess:====>#{mess}=====class_type:==>#{class_type}========JSON parse ====>ex===>#{ e.message }")
      #raise e
    end
    #对于我们的业务来说  在@message 字段中有时是一个数组，数组中的element 为json，有时是一个json
    begin
      if "Array" == class_type
        for mydoc in logs
          #model1
          #cql_cra = parse_cql(mydoc)  ###计划使用自己定义的配置文件解析cql,有待后续实现
          # 不用解析cql，但是使用人员比较难用
          #modle2
          insert_run(mydoc)

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

    if @@calc_count % 1000 == 0
      @logger.info("current calc_count:===>#{@@calc_count}")##还不准确，程序会空跑加，但问题不大日后修改
    end
    # @logger.info("current calc_count:===>#{@@calc_count}")
    return "event  received"


  end

#准备好所有cql,只能从磁盘读取，而且是每次都要读取，这是一个巨大的弊端，需要日后优化，例如存到内存中以文本的方式存！

  def get_cql(mydoc)
    begin
      before_get_cql
      cql_str = File.read(cqlFile_path)
      cql_h = eval(cql_str)
      raise "cql.conf is null" if cql_h.nil?
      return cql_h
    rescue Exception => e
      @logger.warn("error:=======>ex= from  get_cql====>#{e.message}====#{mydoc}")
    end
  end

  def insert_run(mydoc)
    begin
      cql_h = get_cql mydoc
      insert_run_do mydoc, cql_h
    rescue Exception => e
      @logger.warn("error:=======>ex= from  insert_run====>#{e.message}===cql_h===#{cql_h}")
      #raise e
    end
  end

#最好传递mydoc ,抛出异常时可以看下原文
  def insert_run_do(mydoc, cql_h)
    #扩展
    cql = before_execute_cql2 mydoc, cql_h
    ##两种插入模式的选择
    execute_cql_single mydoc, cql  #单条模式，
    #execute_cql_batch_add2 cql
    @@calc_count += 1
  end

  def execute_cql_single(mydoc, cql_str)
    begin
      cql_str.each do |k, v|
        execute_cql k, v
      end
    rescue Exception => e
      @logger.warn("ex====from execute_cql_single=======e===>#{e.message}====mydoc===>#{mydoc}")
      #raise e
    end
  end


  def execute_cql(k = nil, v)
    begin
      rs = @session.execute(v)
      return rs
    rescue Exception => e
      @logger.warn("ex====from execute_cql===next connect_dra====e===>#{e.message}===error insert ========#{k}=========>#{v}<======")
      #抛给上一个调用者，方便打印mydoc
      raise e
      @@calc_count = @@calc_count - 1
      connect_dra
    end
  end


  def before_execute_cql2(mydoc, cql_str)
    begin
      #定制化为复杂的插入扩展点
      for_liuce_count_by_bucket_error mydoc, :cql6, cql_str[:cql6]
      for_liuce_query_traceids mydoc, :cql7, cql_str[:cql7]
      #提高效率去掉定制化k,v
      cql_str.delete(:cql6)
      cql_str.delete(:cql7)
      return cql_str
    rescue Exception => e
      @logger.warn("ex====from before_execute_cql===next connect_dra====e===>#{e.message}===error insert ====================")
      #raise e
    end
  end

#批量执行但是无法精确报错

  def execute_cql_batch_add2(cql_str)
    begin
      batch = @session.batch
      for v in cql_str.values
        batch.add(v)
      end

      #执行
      execute_cql batch
    rescue Exception => e
      @logger.warn("ex====from execute_cql_batch_add2===next connect_dra====e===>#{e.message}===error insert ===cql_str===>#{cql_str}==")
      connect_dra
      #retry
    end
  end


  def for_liuce_count_by_bucket_error(mydoc, k, v)
    begin
      t_error = mydoc['tags'] == nil ? false : (mydoc['tags']['error'] == nil ? false : true)
      bucket = mydoc['timestamp'] / 1440000000
      cql = "select t_error,bucket,count from count_by_bucket_error where t_error=#{t_error} and bucket=#{bucket};"
      rs = execute_cql cql
      @count = 1
      if rs.size == 1
        rs.each do |row|
          @count = row['count'].to_i + 1
        end
      end
      cql = "insert into count_by_bucket_error  (t_error,bucket,count) VALUES (#{mydoc['tags'] == nil ? false : (mydoc['tags']['error'] == nil ? false : true)},#{mydoc['timestamp'] / 1440000000},#{@count})"
      execute_cql k, cql

    rescue Exception => e
      @logger.warn("ex====from for_liuce_count_by_bucket_error===next connect_dra====e===>#{e.message}===error insert =====k:===#{k}=======v:==>#{v}<===cql:===>#{cql}== @count#{@count}=")
      connect_dra
    end

  end

  def for_liuce_query_traceids(mydoc, k, v)

    begin
      traceId = mydoc['traceId'] == nil ? "null" : mydoc['traceId']
      cql = "SELECT trace_id from query_traceids  where trace_id='#{traceId}'"
      rs = execute_cql cql
      if rs.size == 1
        rs.each do |row|
          duration = mydoc['duration'] == nil ? 0 : mydoc['duration']
          @sum_duration = row['sum_duration'].to_i + duration
        end
        execute_cql k, v
      end
      #如果没有查询到则插入第一次的初值mydoc['duration']
      cql = "insert into query_traceids  (trace_id,l_service,sum_duration,t_error,timestamp) values ('#{mydoc['traceId'] == nil ? "null" : mydoc['traceId']}','#{mydoc['localEndpoint']['serviceName'] == nil ? "null" : mydoc['localEndpoint']['serviceName']}',#{mydoc['duration'] == nil ? 0 : mydoc['duration']},#{mydoc['tags'] == nil ? "false" : (mydoc['tags']['error'] == nil ? "false" : "true")},#{mydoc['timestamp'] == nil ? "null" : mydoc['timestamp']});"
      execute_cql k, cql
    rescue Exception => e
      @logger.warn("ex====from for_liuce_query_traceids===next connect_dra====e===>#{e.message}===error insert =====k:===#{k}=======v:==>#{v}<===cql--select:===>#{cql}== @count===>#{@count}")
      connect_dra
    end
  end
#为了扩展复杂的插入要求
  def before_get_cql
    @count = 0 #for liuce  table count_by_bucket_error
    @sum_duration = 0 #for liuce  table query_traceids
  end

end


# class LogStash::Outputs::Mycrassandra