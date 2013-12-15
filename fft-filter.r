library(signal)
library(plyr)

daily <- function(x) { if (length(x) > 5) {median(x, na.rm=T) } else { NA}  }
filter <- function(x) runmed(x, 13)

data <- read.csv('data.csv', header=F)
names(data) <- c('plot','depth','datetime','vwc')
data$datetime <- as.POSIXct(data$datetime)

data$date <- as.Date(format(data$datetime, '%Y-%m-%d'))
filtered <- ddply(data, .(plot, depth), transform, filtered=filter(vwc))
reduced <- ddply(filtered, .(plot, depth, date), summarize, daily=daily(filtered))

write.csv(reduced[complete.cases(reduced),],'filtered.csv', row.names=FALSE, col.names=FALSE)
