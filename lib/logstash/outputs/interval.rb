class Time_tools
  public

  def interval(leval,durasec = 10, calc_count)
    if durasec > 60
      raise "please  input 1~60 "
    end

    while true
      n1 = level_to leval

      sleep(1)

      flagOut = true
      flagIn = true #内层大休眠 保证效率不消耗cpu
      while flagOut
        cur = level_to  leval
        dura = cur - n1
        if cur < n1
          dura = 60 - n1 + cur #当前时间处于60s过度时间
        end

        if (dura >= durasec)
          # block.call
          puts " #{Time.now}=============#{durasec} sec time go...=====current count:===>#{calc_count}<==="
          flagOut = false #外层循环
        elsif flagIn
          flagIn = false
          sleep(((durasec - 1) - dura) * 1)
        else
          #puts "=====sleep 1...#{Time.now}.."
          sleep(1)
        end
      end
    end
  end

  def level_to(level)

    if("min" ==level)
      t =  Time.now.min
    elsif "sec" == level
      t = Time.now.sec
    else
      raise "only support min or sec of level"
    end
  end


end

# #class end time tool
# puts "===========begin"
# puts Time.now
#
# th = Thread.new do
#   tool = Time_tools.new
#   tool.interval 'sec' ,5,0
# end
#
# sleep(22)
# puts "==============over"
