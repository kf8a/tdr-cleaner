#Clean tdr data and produce daily gap filled values 
require 'sequel'
require 'csv'
require 'etc'
require 'yaml'
require 'logger'
require 'hamster'

class CleanOutliers

  class << self
    def process(data, cutoff)
      result = []
      data.each.with_index do |datum,i|
        if i > 1 and i < data.length - 2
          offset = offset(data[i-1], datum, data[i+1])
          if offset.abs < cutoff
            result << datum
          else
            if offset > cutoff
              # check for rain
              rain = TDR["select sum(rain_mm) from weather.lter_five_minute_a where datetime between #{datum[:datetime]} and #{datum[:datetime]} + interval '2 day'"]
              result << datum if rain > 0
            end
          end
        end
      end
      result.compact
    end

    def offset(prev, data, following)
      ((prev[:topvwc] + following[:topvwc])/2 - data[:topvwc])
    end

    def mean(data)
      data.inject(0) {|accum, i| accum + i[:topvwc]}/data.length.to_f
    end

    def sample_variance(data)
      m = mean(data)
      sum = data.inject(0){|accum, i| accum +(i[:topvwc]-m)**2 }
      sum/(data.length - 1).to_f
    end

    def standard_deviation(data)
      Math.sqrt(sample_variance(data))
    end

  end
end


class DailyValue

  class << self
    def process(data)
      days = data.group_by {|d| d[:datetime].to_date }
      daily = days.collect do |day|
        next if day[1].length < 5 
        median(day[1]) 
      end
      daily.compact
    end

    def median(x)
      sorted = x.sort {|x,y| x[:vwc] <=> y[:vwc] }
      mid = x.size/2
      datum = sorted[mid]
      datum[:count] = x.length
      datum[:date] = datum[:datetime].to_date
      datum 
    end
  end
end

class GapFill
  class << self
#     def process(data, plot=nil, depth=nil)
#       # diff(data)

#       # http://weblog.bocoup.com/padding-time-series-with-r/
#       # con=Rserve::Connection.new

#       max_date = data.max {|a,b| a[:datetime] <=> b[:datetime]}
#       min_date = data.min {|a,b| a[:datetime] <=> b[:datetime]}

#       result = []
#       hour = min_date[:datetime]
#       while hour < max_date[:datetime]
#       begin
#         have_date = data.select {|x| x[:datetime] == hour}
#         unless have_date[0]
#           have_date = [{:datetime => hour, :plot => plot, :depth => depth}]
#         end
#         p have_date[0]
#         result << have_date[0]
#         hour += 3600
#       end

#       # x = con.eval('seq(min_date, max_date, by="hour")'

#       # compute  a value for the missing data by linear interpolation 
#       # if the gap is less than 3 days
#       result
#     end

    def diff(data)
      b = data.dup
      b.shift
      result = {}
      data.each.with_index do |datum, i|
        next if i > b.length - 1
        diff =  b[i][:date] - datum[:date]
        if diff == 1
          result[datum[:date]] = datum
        elsif  diff > 1 and diff < 5
          # compute numbers for every day

          range = ((b[i][:date] + 1)..datum[:date])
          range.each do |d|
            new_data = datum.dup
            new_data[:topvwc] = nil
            result[d[:date]] = new_data
          end
        end
        datum[:diff] = diff.to_f
      end
    end

    def small_gap(diffs)
      # diffs.select {|x| x[:diff] > 1 and x[:diff] < 3}
    end

  end
end


if __FILE__==$0
  user = Etc.getlogin

  credentials = File.open(File.join(Dir.home(user),'credentials.yaml')) {|y| YAML::load(y)}


  TDR = Sequel.postgres(:database       => 'metadata',
                        :host           => 'thetford.kbs.msu.edu',
                        # :host         => 'localhost',
                        # :port         => 5430,
                        # :logger         => [Logger.new($stdout)],
                        :user           => credentials['username'],
                        :password       => credentials['password'])

  CLEANED = TDR[:glbrc__cleaned_daily_tdr_data]

  Sequel.extension :pg_array

  locations = TDR['select * from weather.glbrc_tdr_locations']
  locations.each do |location|
    next if ['BT','GT','FT'].include? location[:plot]
    data = TDR[%q{select plot, depth, glbrc_tdr_data.datetime, topvwc from weather.glbrc_tdr_data join weather.glbrc_tdr_locations on glbrc_tdr_data.location_id = glbrc_tdr_locations.id where plot not in ('BT','GT','FT') and glbrc_tdr_locations.id = ? and topvwc < 0.5 and topvwc > 0.05 order by datetime}, location[:id]].all

    CSV.open('data.csv', 'w') do |f|
      data.each do |datum|
        f << [datum[:plot], datum[:depth], datum[:datetime], datum[:topvwc]]
      end
    end
    system("cp data.csv #{location[:plot]}-#{location[:depth]}.csv")

    system('rm -f filtered.csv')
    system('Rscript fft-filter.r &> /dev/null')
    system "cp filtered.csv #{location[:plot]}-#{location[:depth]}-filtered.csv"

    data = []
    CSV.foreach('filtered.csv','r') do | row |
      plot, depth, date, topvwc, vwc = row
      next if plot == 'plot'
      data << {:plot => plot, :depth => depth, :datetime => DateTime.parse(date), :topvwc => topvwc.to_f, :vwc => vwc.to_f}
    end

    daily = DailyValue.process(data)
    clean_daily = CleanOutliers.process(daily, 0.03)

    # delete the old data
    TDR['delete from glbrc.cleaned_daily_tdr_values'] 

    CSV do |stdout|
      clean_daily.each do |day|
        CLEANED.insert(:plot => day[:plot], :depth => day[:depth],
                       :date => day[:date], :vwc => day[:vwc])
        stdout << [day[:plot], day[:depth], day[:date], day[:vwc] ]
      end
    end

  end
  # Dir['*.yaml'].each do |file|
  #   plot, depth = file.split('.')[0].split('-')
  #   data = YAML.load(File.read(file))
  #   # cleaned   = CleanOutliers.process(data, 0.005)
  #   # daily     = DailyValue.process(data)
  #   # clean_daily = CleanOutliers.process(daily, 0.007 )

  #   filled = GapFill.process(data, plot, depth)
  #   # filled_daily = GapFill.process(daily)
  #   filled.each do |day|
  #     # puts [day[:plot], day[:depth], day[:datetime], day[:topvwc], day[:offset] ].join(',')
  #     puts [day[:plot], day[:depth], day[:datetime], day[:topvwc] ].join(',')
  #   end
  #   # data.each do |day|
  #   #   # puts [day[:plot], day[:depth], day[:datetime], day[:topvwc], day[:offset] ].join(',')
  #   #   puts [day[:plot], day[:depth], day[:datetime], day[:topvwc] ].join(',')
  #   # end
  # end
end
