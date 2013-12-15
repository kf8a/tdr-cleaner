#Clean tdr data and produce daily gap filled values 
require 'sequel'
require 'date'
require 'csv'
require 'etc'
require 'yaml'
require 'logger'

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
      ((prev[:vwc] + following[:vwc])/2 - data[:vwc])
    end

    def mean(data)
      data.inject(0) {|accum, i| accum + i[:vwc]}/data.length.to_f
    end

    def sample_variance(data)
      m = mean(data)
      sum = data.inject(0){|accum, i| accum +(i[:vwc]-m)**2 }
      sum/(data.length - 1).to_f
    end

    def standard_deviation(data)
      Math.sqrt(sample_variance(data))
    end

  end
end


class GapFill
  # http://weblog.bocoup.com/padding-time-series-with-r/

  class << self
    def process(data)
      result = []
      data.each_with_index do |datum, i|
        result << datum
        next if i < 1
        next if datum[:date] - 1 == data[i-1][:date]
        if datum[:date] - 7 < data[i-1][:date]
          # fillable 
          # check for rain and more than one day gap
					gap_rain = rain(data[i-1][:date], datum[:date])
					if gap_rain	> 0 && datum[:date] - 3 > data[i-1][:date]
						fill_empty(result, data[i-1][:date], datum[:date], datum)
					else
						fills = fill(data[i-1], datum)
						fills.each do |fill|
							result << fill
						end
					end
				else # big gap
					fill_empty(result, data[i-1][:date], datum[:date],datum)
				end
			end
      result.sort {|a,b| a[:date] <=> b[:date]}
    end

		def fill_empty(result, from, to, datum)
			((from + 1)..(to - 1)).each do |d|
				result << {:plot => datum[:plot],:depth => datum[:depth], :date => d}
			end
		end

    def rain(from, to)
      rain = TDR["select sum(rain_mm) from weather.lter_five_minute_a where datetime between ? and ?", from, to].first
      if rain[:sum].nil?
        rain[:sum] = 0
      end
      rain[:sum]
    end

    def fill(from, to)
      slope = (from[:vwc] - to[:vwc])/(from[:date].to_time.to_i - to[:date].to_time.to_i)
      b = from[:vwc] - slope * from[:date].to_time.to_i

      (from[:date]+1..to[:date]-1).collect do |day|
        d = day.to_time.to_i
        {:date => day, :plot => from[:plot],
          :depth => from[:depth],
          :vwc => slope * d + b, :flag => 'E'}
      end
    end

  end
end

class AddRain
	def self.process(data)
		data.each do | datum |
			rain = TDR["select sum(rain_mm) from weather.lter_five_minute_a where date_trunc('day',datetime) = ?", datum[:date]].first
			datum[:rain] = rain[:sum]
		end
		data
	end
end

if __FILE__==$0
  user = Etc.getlogin

  credentials = File.open(File.join(Dir.home(user),'credentials.yaml')) {|y| YAML::load(y)}

  TDR = Sequel.postgres(:database       => 'metadata',
                        :host           => 'thetford.kbs.msu.edu',
                        # :logger         => [Logger.new($stdout)],
                        :user           => credentials['username'],
                        :password       => credentials['password'])

  CLEANED = TDR[:glbrc__cleaned_daily_tdr_data]

  Sequel.extension :pg_array

  # delete the old data
  #TDR['delete from glbrc.cleaned_daily_tdr_values'] 

  locations = TDR['select * from weather.glbrc_tdr_locations']
  locations.each do |location|
    data = TDR[%q{select plot, depth, glbrc_tdr_data.datetime, topvwc from weather.glbrc_tdr_data join weather.glbrc_tdr_locations on glbrc_tdr_data.location_id = glbrc_tdr_locations.id where plot not in ('BT','GT','FT') and glbrc_tdr_locations.id = ? and topvwc < 0.5 and topvwc > 0.05 order by datetime}, location[:id]].all

    CSV.open('data.csv', 'w') do |f|
      data.each do |datum|
        f << [datum[:plot], datum[:depth], datum[:datetime], datum[:topvwc]]
      end
    end
    # system("cp data.csv #{location[:plot]}-#{location[:depth]}.csv")

    system('rm -f filtered.csv')
    system('Rscript fft-filter.r > /dev/null')
#    system "cp filtered.csv #{location[:plot]}-#{location[:depth]}-filtered.csv"

    data = []
    CSV.foreach('filtered.csv','r') do | row |
      plot, depth, date, vwc = row
      next if plot == 'plot'
      data << {:plot => plot, :depth => depth, :datetime => Date.strptime(date,'%Y-%m-%d'), :vwc => vwc.to_f}
    end

    clean_daily = CleanOutliers.process(data, 0.03)
    filled = GapFill.process(clean_daily)
    #with_rain = AddRain.process(filled)

    CSV do |stdout|
      filled.each do |day|
        # CLEANED.insert(:plot => day[:plot], :depth => day[:depth],
        #                :date => day[:date], :vwc => day[:vwc])
        stdout << [day[:plot], day[:depth], day[:date], day[:vwc], day[:flag], day[:rain] ]
      end
    end

  end
end
