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
