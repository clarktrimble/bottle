
## NewRecord

- awkward because implementations must return bottle.Record, not concrete
- could take a pointer to concrete via "any" param and marshal into it
- or maybe just return bytes and let upstream deal with it, yeah
