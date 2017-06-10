## Definition

in-range records: 

- the last operation for this record is an insert or update
- and after the last operation, the current key for this record is in the range

out-of-range records:

- the last operation for this record is an insert or update
- and after the last operation, the current key for this record is out of range

dead records:

- the last operation for this record is a delete
- the record only persist from insert to the current delete

## How to know an operation is the last operation

in-range active keys: 

- changed from an in-range record update(backwards), typically an update
- if there is any previous key => current key change, remove current key 
- insert previous key
- if there is an insert, that is what we want to find out

out-of-range active keys    
- changed from an in-range record update(backwards), typically an update
- if there is any previous key => current key change, remove current key 
- insert previous key
- if there is an insert, we do nothing
    
dead keys:

- changed from a record delete(backwards), a delete
- if there is any previous key => current key change, replace dead key with previous key
- if met insert, remove dead key from dead key set

criteria for last-operation categorization: 

- if it is a delete, definitely the last operation for a record
- else if it is not active in in-range inspectors, out-of-range inspectors and dead inspectors, 
then it is the last operation for a record.