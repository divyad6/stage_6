#include "catalog.h"
#include "query.h"
#include "heapfile.h"


/*
 * Deletes records from a specified relation.
 *
 * Returns:
 * 	OK on success
 * 	an error code otherwise
 */

const Status QU_Delete(const string & relation, 
		       const string & attrName, 
		       const Operator op,
		       const Datatype type, 
		       const char *attrValue)
{
	// part 6

	// try to get attribute descriptor
	AttrDesc attrDesc; 
	Status status = attrCat->getInfo(relation, attrName, attrDesc);
    	if (status != OK) {
    	    return status;  
    	}

	// do a filtered scan to find which records to delete
	HeapFileScan scan(relation, status);
	if (status != OK) {
    	    return status;  
    	}
	scan.startScan(attrDesc.attrOffset, attrDesc.attrLen, type, attrValue, op);

	// iterate through scan and delete the matching records
	RID rid; 
	while (scan.scanNext(rid) == OK) {
		status = scan.deleteRecord();
		if (status != OK) { // problem when trying to delete record
			scan.endScan();
			return status;
		}
	}

	scan.endScan();

	return OK;

}


