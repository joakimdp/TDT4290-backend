# TODO: Implement this

import apis.regobs.regobs_fetcher as r
import apis.skredvarsel.skredvarsel as r

print(r.SkredvarselFetcher().fetch())
print(r.RegobsFetcher().fetch())
