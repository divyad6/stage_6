#include "catalog.h"
#include "query.h"


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
	std::cout << "Doing QU_Delete\n";

	Status status;

	// try to get attribute descriptor
	AttrDesc attrDesc; 
	if (attrValue != nullptr) {
		status = attrCat->getInfo(relation, attrName, attrDesc);
    	if (status != OK) {
    	    return status;  
    	}
	}

	// get relation info
	RelDesc relDesc;
	status = relCat->getInfo(relation, relDesc);
	if (status != OK) {
		return status;
	}

	// do a filtered scan to find which records to delete
	HeapFileScan scan(relation, status);

	int i;
	float f;

	if (attrValue == NULL) {
		scan.startScan(0, 0, STRING, NULL, EQ);
	} else {
		// convert attrValue to correct type and start scan
		switch(type) {
			char* value;
			case INTEGER:
				value = (char *)attrValue;
				i = atoi(value);
				status = scan.startScan(attrDesc.attrOffset, attrDesc.attrLen, type, (char *)&i, op);
				break;
			case FLOAT:
				value = (char *)attrValue;
				f = atof(value);
				status = scan.startScan(attrDesc.attrOffset, attrDesc.attrLen, type, (char *)&f, op);
				break;
			default:
				scan.startScan(attrDesc.attrOffset, attrDesc.attrLen, type, attrValue, op);
		}

		if (status != OK) {
			return status;
		}
	}

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