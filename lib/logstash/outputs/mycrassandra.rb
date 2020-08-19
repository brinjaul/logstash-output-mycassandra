# encoding: utf-8
require "logstash/outputs/base"
require 'cassandra'
require 'json'

# An mycrassandra output that does nothing.
class LogStash::Outputs::Mycrassandra < LogStash::Outputs::Base
  attr_accessor :result, :keyspace, :cluster, :session,:table
  config_name "mycrassandra"
  config :cra_database, :validate => :string, :default => "bootwiki"
  config :cra_table, :validate => :string, :default => "zp"
  config :cra_name, :validate => :string, :default => "root"
  config :cra_pwd, :validate => :string, :default => "123456"
  config :cra_hosts, :validate => :string, :default => "192.168.244.10"
  @@calc_count=0
  public

  def register
    puts "=========begin  register==================="
    @cluster = Cassandra.cluster(username: cra_name,
                                 password: cra_pwd,
                                 hosts: [cra_hosts])
    @keyspace = cra_database
    @table= cra_table
    @session = @cluster.connect(@keyspace) # create session, optionally scoped to a keyspace, to execute queries
    puts "=========end  register==================="
  end


  public

  def receive(event)
    puts "========begin receive========================="
    myobject = {}.merge(event.to_hash)
    #puts "myobject:===================#{myobject}"

    mydoc = myobject['doc']

    puts "myobject['doc']:======#{mydoc.class}=============#{myobject['doc']}"

    puts "=after=mydoc['localEndpoint']==============>#{mydoc['localEndpoint']}"
    local_p = check_lp(mydoc)
    puts "=after=mydoc['localEndpoint']==============>#{mydoc['localEndpoint']}==============local_p=====>#{local_p}"

    puts "before==mydoc['remoteEndpoint']==============>#{mydoc['remoteEndpoint']}"
    remove_p = check_rp(mydoc)
    puts "after==mydoc['remoteEndpoint']============>#{mydoc['remoteEndpoint']}==========>remove_p:======>#{remove_p}"

    tags_p =check_tags(mydoc)
    puts "after==mydoc['tags']============>#{mydoc['tags']}====>tags_p:======>#{tags_p}"
    sql_cra = "INSERT INTO #{@table} (traceId,name,parentId,id,kind,timestamp,timestamp_mills,duration,debug,shared,localEndpoint,remoteEndpoint,tags) VALUES ('#{mydoc['traceId'] == nil ? 'null' : mydoc['traceId']}','#{mydoc['name'] == nil ? 'null' : mydoc['name']}','#{mydoc['parentId'] == nil ? 'null' : mydoc['parentId']}','#{mydoc['id'] == nil ? 'null' : mydoc['id']}','#{mydoc['kind'] == nil ? 'null' : mydoc['kind']}',#{mydoc['timestamp'] == nil ? 'null' : mydoc['timestamp']},#{mydoc['timestamp_mills'] == nil ? 'null' : mydoc['timestamp_mills']},#{mydoc['duration'] == nil ? 'null' : mydoc['duration']},#{mydoc['debug'] == nil ? 'null' : mydoc['debug']},#{mydoc['shared'] == nil ? 'null' : mydoc['shared']},#{local_p},#{remove_p},#{tags_p})"
    puts sql_cra
    @session.execute(sql_cra) # fully asynchronous api
    @@calc_count += 1
    puts  "current calc_count:===>#@@calc_count"
    puts "========end receive========================="
    return "event  received"
  end # def event

  def check_lp(mydoc)
    begin
      unless mydoc['localEndpoint'] == nil
        pore = mydoc['localEndpoint']['port']
        mydoc['localEndpoint']['port'] = "#{pore}"
        local_p = mydoc['localEndpoint'].to_json.gsub!(/["]/, '\'') == nil ? 'null' : mydoc['localEndpoint'].to_json.gsub!(/["]/, '\'')
      else
        local_p ='null'
      end
    rescue Exception => e
      puts "check  localEndpoint"
      puts e.message
      raise e
    end

  end

  def check_rp(mydoc)
    begin
      unless mydoc['remoteEndpoint'] == nil
        por = mydoc['remoteEndpoint']['poprt']
        mydoc['remoteEndpoint']['port'] = "#{por}"
        remove_p = mydoc['remoteEndpoint'].to_json.gsub!(/["]/, '\'') == nil ? 'null' : mydoc['remoteEndpoint'].to_json.gsub!(/["]/, '\'')
      else
        remove_p = 'null'
      end
    rescue Exception => e
      puts "check remoteEndpoint"
      puts e.message
      raise e
    end
  end

  def check_tags(mydoc)
    begin
      unless mydoc['tags'] == nil
        tags_p = mydoc['tags'].to_json.gsub!(/["]/, '\'') == nil ? 'null' : mydoc['tags'].to_json.gsub!(/["]/, '\'')
      else
        tags_p = 'null'
      end
    rescue Exception => e
      puts "tags only store Map<text,text>!!!!"
      puts e.to_s
    end
  end
end # class LogStash::Outputs::Mycrassandra
