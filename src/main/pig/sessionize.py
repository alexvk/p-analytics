#######################
# Sessionize Python UDF
#######################
# process - find the time spent on pages and rank them
# The input should be the (timestamp, page) pairs
@outputSchema("y:bag{t:tuple(rank:int,len:long,page:chararray)}")
def process(bag):
  outBag = []
  nextstamp = 0L
  times_on_page = {}
  timestamp = long(view[0])
  for view in bag:
    if nextstamp != 0L:
      times_on_page[view[1]] = times_on_page.get(view[1], 0) + nextstamp - timestamp
    nextstamp = timestamp
  total_times = []
  for k, v in times_on_page.iteritems():
    total_times.append((v, k))
  sorted_times = sorted(total_times, key = lambda x : x[0], reverse=True)
  rank = 0
  for pair in sorted_times:
    rank += 1
    tup=(rank, pair[0], pair[1])
    outBag.append(tup)
  return outBag
