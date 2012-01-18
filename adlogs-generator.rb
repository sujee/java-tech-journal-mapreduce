#!/usr/bin/env ruby


## generates mock adlogs
# log format
# timestamp (in ms), user, action_id, domain, campaign_id

## multi threaded version... ruby doesn't have native threads, so threading has no effect
## execute with jruby to see true threading
## jruby adlogs-generator.rb

# config
days=5
entries=1000
# end config

time_inc = (24.0*3600)/entries
#puts "time inc : #{time_inc}"



domains = %w[facebook.com  yahoo.com   google.com   zynga.com    wikipedia.org   sf.craigslist.org   twitter.com    amazon.com    flickr.com    cnn.com      usatoday.com      npr.org    foxnews.com      comedycentral.com   youtube.com   hulu.com   bbc.co.uk  nytimes.com   sfgate.com   funnyordie.com]

threads = []

0.upto(days-1) do |day|

  threads << Thread.new(day) do |myday|
    start_ts = Time.local(2011, 1, 1) + myday * 24 * 3600
    end_ts = Time.local(2011, 1, 1 , 23, 59, 59) + myday * 24 * 3600
    filename = start_ts.strftime("%Y-%m-%d") + ".log"
    puts "writing #{filename}"
    last_ts = start_ts
    File.open(filename, 'w') do |f|
      while last_ts < end_ts
        last_ts =  last_ts + time_inc

        timestamp = (last_ts.to_f*1000).to_i

        # skew the domains towards the start, so they are not uniformly, randomly
        # distributed.  makes for interesting stats
        domain_index = rand(domains.size) - rand(3)
        domain_index = 0 if domain_index < 0
        domain =  domains[domain_index]
        user = rand(1000000) + 1
        action = rand(2)+1

        campaign_id = rand(20) + 1

        logline = "#{timestamp},#{user},#{action},#{domain},#{campaign_id}"
        #puts logline
        f.write "#{logline}\n"
      end
    end
  end
end

threads.each {|t| t.join}
