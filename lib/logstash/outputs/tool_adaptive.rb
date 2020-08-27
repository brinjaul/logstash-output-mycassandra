require 'json'


class Tool_adaptive
#引号 quotes  2--->1
  public

  def hash_adaptive_mapCra2(hstr)
    return nil if hstr == nil
    if "Hash" == hstr.class.to_s #  Hash =>"Hash"
      ##为了转义一些特殊字符
      realstr = hstr.to_json
      rs = realstr.gsub(/["]/, '\'')
    else
      raise "q2Toq1ForHash mentod  access only Hash type"
    end
  end
  def hash_adaptive_mapCra(hstr)
    return nil if hstr == nil
    if "Hash" == hstr.class.to_s #  Hash =>"Hash"
      ##为了转义一些特殊字符
      realstr = hstr.to_json
      rs = realstr.gsub(/["]/, '$$')
    else
      raise "q2Toq1ForHash mentod  access only Hash type"
    end
  end

#引号 quotes  1-->2
  def q1Toq2ForStr(str)
    return nil if str == nil
    if "String" == str.class.to_s #  Hash =>"Hash"
      realstr = str
      rs = realstr.gsub(/[']/, '\"')
    else
      raise "q1Toq2ForStr mentod  access only String type"
    end
  end

#适用 于自定义类型插入时
  def hash_adaptive_customCra(hstr) #带有字符的
    return nil if hstr == nil
    begin
      if "Hash" == hstr.class.to_s
        #拼接字符串  注意区分是否含有字符串！！有字符串的加上单引号
        strbegin = "{"
        str = ""
        hstr.each do |keys, values|
          type = values.class.to_s
          if "Hash" == type
            str = str + noQuouteForKey(values)
          elsif "String" != type ##这里一定是在Hash判断之后  否则结果不同，可以使用 例子来测试，test5 = {"port":3366,"name":"fjp","flag":true,"net":{"net":"vnet","p":6633}}
            str = str + "#{keys}:" + "#{values},"
          else
            str = str + "#{keys}:" + "'#{values}',"
          end
        end
        str.chop!
        strend = "}"
        rs = strbegin + str + strend
      else
        raise "noQuouteForHash mentod  access only Hash type or nest Hash"
      end
    rescue Exception => e
      puts("==ex===from   noQuouteForHash =======#{e.message}")
    end
  end

end

#end class









