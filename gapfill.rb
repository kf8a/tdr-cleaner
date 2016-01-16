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
						fills = fill_by_linear_interpolation(data[i-1], datum)
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

    def fill_by_linear_interpolation(from, to)
      slope = (from[:vwc] - to[:vwc])/(from[:date].to_time.to_i - to[:date].to_time.to_i)
      b = from[:vwc] - slope * from[:date].to_time.to_i

      (from[:date]+1..to[:date]-1).collect do |day|
        d = day.to_time.to_i
        {:date => day, :plot => from[:plot],
          :depth => from[:depth],
          :vwc => slope * d + b, :flag => 'E'}
      end
    end

    def fill_by_correlation(from, to)
      # go back a day or two
      # check if there is no gap in other series
      # linear fit between the current data and the data in the other series
      # select fit with the best r2
      # compute missing data based on the fit
    end

  end
end
