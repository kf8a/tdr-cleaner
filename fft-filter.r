library(signal)
data <- read.csv('data.csv', header=F)
names(data) <- c('plot','depth','date','vwc')
f <- fir1(64, 0.01, type='low', scale=TRUE)
data$filtered <- runmed(data$vwc,13)
write.csv(data,'filtered.csv', row.names=FALSE, col.names=FALSE)
