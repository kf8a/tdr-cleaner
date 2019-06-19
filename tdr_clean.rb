# Clean tdr data and produce daily gap filled values
require 'sequel'
require 'date'
require 'csv'
require 'netrc'

require_relative 'clean_outliers.rb'
require_relative 'gapfill.rb'

# Class to return rainfall
class AddRain
  def self.process(data)
    data.each do |datum|
      rain = TDR["select sum(rain_mm) from weather.lter_five_minute_a \
                 where date_trunc('day',datetime) = ?", datum[:date]].first
      datum[:rain] = rain[:sum]
    end
    data
  end
end

def write_out_datafile(data)
  CSV.open('data.csv', 'w') do |f|
    data.each do |datum|
      f << [datum[:plot], datum[:depth], datum[:datetime], datum[:topvwc]]
    end
  end
end

if $PROGRAM_NAME == __FILE__

  netrc = Netrc.read(Dir.home + '/.netrc.gpg')
  creds = netrc['database']

  TDR = Sequel.postgres(database: 'metadata',
                        host: 'localhost',
                        user: cred['login'],
                        password: cred['password'])

  CLEANED = TDR[Sequel.qualify('glbrc', 'cleaned_daily_tdr_data')]

  # delete the old data
  # TDR['delete from glbrc.cleaned_daily_tdr_values']

  # locations = TDR["select * from weather.glbrc_tdr_locations where \
  # plot in ('BT','GT','FT')"]
  locations = TDR['select * from weather.glbrc_tdr_locations']
  locations.each do |location|
    query = <<~HEREDOC
      select plot, depth, glbrc_tdr_data.datetime, topvwc
      from weather.glbrc_tdr_data join weather.glbrc_tdr_locations
      on glbrc_tdr_data.location_id = glbrc_tdr_locations.id
      where glbrc_tdr_locations.id = ? and topvwc < 0.5
      and topvwc > 0.05 order by datetime
    HEREDOC
    data = TDR[query, location[:id]].all

    write_out_datafile(data)

    system('rm -f filtered.csv')
    system('Rscript fft-filter.r > /dev/null')

    data = []
    CSV.foreach('filtered.csv', 'r') do |row|
      plot, depth, date, vwc = row
      next if plot == 'plot'
      data << { plot: plot, depth: depth,
                date: Date.strptime(date, '%Y-%m-%d'), vwc: vwc.to_f }
    end

    clean_daily = CleanOutliers.process(data, 0.03)
    filled = GapFill.process(clean_daily)
    with_rain = AddRain.process(filled)

    CSV do |stdout|
      with_rain.each do |day|
        # CLEANED.insert(:plot => day[:plot], :depth => day[:depth],
        #                :date => day[:date], :vwc => day[:vwc])
        stdout << [day[:plot], day[:depth], day[:date], day[:vwc],
                   day[:flag], day[:rain]]
      end
    end
  end
end
