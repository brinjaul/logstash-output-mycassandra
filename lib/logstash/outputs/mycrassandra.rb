# encoding: utf-8
require "logstash/outputs/base"
require 'cassandra'
require 'json'

# An mycrassandra output that does nothing.
class LogStash::Outputs::Mycrassandra < LogStash::Outputs::Base
  attr_accessor :result, :keyspace, :cluster, :session
  config_name "mycrassandra"
  config :cra_database, :validate => :string, :default => "zipkin2"
  # config :cra_table, :validate => :string, :default => "zipkin2"
  config :cra_name, :validate => :string, :default => "root"
  config :cra_pwd, :validate => :string, :default => "123456"
  config :cra_hosts, :validate => :string, :default => "192.168.244.10"
  config :cqlFile_path, :validate => :string, :default => "/cql.conf"

  @@calc_count = 0

  public

  def register
    puts "=========begin  register==================="
    @cluster = Cassandra.cluster(username: cra_name,
                                 password: cra_pwd,
                                 hosts: [cra_hosts])
    @keyspace = cra_database
    @session = @cluster.connect(@keyspace) # create session, optionally scoped to a keyspace, to execute queries
    @cluster.each_host do |host| # automatically discovers all peers
      puts "Host #{host.ip}: id=#{host.id} datacenter=#{host.datacenter} rack=#{host.rack}"
      puts "=========end  register==================="
    end
  end

  public

  def receive(event)
    puts "=====================================================begin receive================================================"
    begin
      myobject = {}.merge(event.to_hash)
      puts "myobject:====#{myobject.class}===============#{myobject}"
    rescue Exception => e
      puts "parse event ====>ex===>#{ e.message }"
      raise e
    end
    mess = myobject['message'].to_s
    puts "begin==#{mess.class}========mess=====================>#{mess}<============mess=====================end"
    logs = JSON.parse mess.gsub(/\\/, '')
    class_type = logs.class.to_s
    puts "class_type=====#{class_type}"
    if "Array" == class_type
      puts "check arr_log-class #{logs.class}========test element: #{logs[0]['traceId']} "
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
    puts "current calc_count:===>#{@@calc_count}"
    puts "========end receive========================="
    return "event  received"
  end

  def insert_run(mydoc)
    begin

      cql_str =  File.read(cqlFile_path)
      cql_h = eval(cql_str)
      puts cql_h.class
      for v in cql_h.values
        puts v
        @session.execute(v)
      end
    rescue Exception => e
      puts "insert_run  method raise ex ====>#{e.message}"
      raise e
    end
  end
end


# class LogStash::Outputs::Mycrassandra