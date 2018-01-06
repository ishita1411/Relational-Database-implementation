#include "rbfm.h"
#include <math.h>


RecordBasedFileManager* RecordBasedFileManager::_rbf_manager = 0;

RecordBasedFileManager* RecordBasedFileManager::instance() {
	if (!_rbf_manager)
		_rbf_manager = new RecordBasedFileManager();

	return _rbf_manager;
}

RecordBasedFileManager::RecordBasedFileManager() {
    pfm = PagedFileManager::instance();

}

RecordBasedFileManager::~RecordBasedFileManager() {
}

RC RecordBasedFileManager::createFile(const string &fileName) {
	return pfm->createFile(fileName);
}

RC RecordBasedFileManager::destroyFile(const string &fileName) {
	return pfm->destroyFile(fileName);
}

RC RecordBasedFileManager::openFile(const string &fileName,
		FileHandle &fileHandle) {
	return pfm->openFile(fileName, fileHandle);
}

RC RecordBasedFileManager::closeFile(FileHandle &fileHandle) {
	return pfm->closeFile(fileHandle);
}

RC RecordBasedFileManager::insertRecord(FileHandle &fileHandle,
		const vector<Attribute> &recordDescriptor, const void *data, RID &rid)
{
    //inistializing offsets to a. "free space location" (F) address and b. "record count location" (N) address
    unsigned short freeSpaceLocation = (PAGE_SIZE - 1 - sizeof(unsigned short));
    unsigned short recCountLocation = (PAGE_SIZE - 1 - (2 * sizeof(unsigned short)));



	int i = 0;
	bool isNullBit;
	int ctr = 0;
	unsigned short lenOfData = 0;
	unsigned short addressArray[recordDescriptor.size()];
	unsigned short locationsLen = (unsigned short) (recordDescriptor.size() * sizeof(unsigned short));
	//cout << "inside insert ";
	//1. Null bit indicator length calculation (in bytes)
	size_t nullBytes = (size_t) ceil((double) recordDescriptor.size() / CHAR_BIT);

	//2. Preparing null-indicator array of length nullByte bytes and putting the "indicator-data" into it
	unsigned char nullIndicator[nullBytes];
	memcpy(nullIndicator, data, nullBytes);

	//3. Checking if the bit is 1 or 0; 1 means null!
	for (i = 0; i < recordDescriptor.size(); i++)
    {
		size_t temp1 = nullIndicator[ctr / 8];
		size_t temp2 = 1 << (7 - (ctr % 8));
		isNullBit = temp1 & temp2;
		ctr++;

		if (isNullBit) //true means there is null value
		{
			addressArray[i] = 0;
			continue;
		}
		addressArray[i] =
				(unsigned short) (nullBytes + locationsLen + lenOfData);

		//4. Checking the data type of records and storing their lengths
        if(recordDescriptor[i].type == TypeInt)
        {
            lenOfData = lenOfData + sizeof(int);

        }
        else if(recordDescriptor[i].type == TypeReal)
        {
            lenOfData = lenOfData + sizeof(float);

        }
        else
        {
            int tempVar = 0;
            memcpy(&tempVar, (char*) data + nullBytes + lenOfData, sizeof(int));
            lenOfData = lenOfData + sizeof(int);
            lenOfData = lenOfData + tempVar;

        }

	}
	//cout<<"Inside insert 2";
	//printf("Length of data : %d",lenOfData);
	//5. Record = a+b+c a. Null Indicator b. AddressArray (Directory of locations) c. Actual data;
	size_t totalDataLen = nullBytes + locationsLen + lenOfData;
	void *record = malloc(totalDataLen);
	memcpy(record, data, nullBytes); //a. Copying null-indicator bits of data
	memcpy((char *) record + nullBytes, addressArray, locationsLen); //copying address-directory
	memcpy((char *) record + nullBytes + locationsLen, (char *) data + nullBytes, (size_t) (lenOfData)); //copying actual data

//	printf("Length total : %d",totalDataLen);
	//6. Find a page to insert the record
    unsigned pageNum;
	pageNum = 0;
	void *page = malloc(PAGE_SIZE);


	//bool isSpace = false;

	//6a. to find if page exists:
	/*if (fileHandle.getNumberOfPages() > 0) {
		int numOfPages = fileHandle.getNumberOfPages();
		for (pageNum = 0; pageNum < numOfPages; pageNum++)
        {
			fileHandle.readPage(pageNum, page);
			memcpy(&freeSpaceAddress, (char *) page + FREE_SPACE_LOCATION,
					sizeof(unsigned short)); //value of the offset till which record is written
			memcpy(&recordCount, (char *) page + RECORD_COUNT_LOCATION,
					sizeof(unsigned short));//value of the record count (stored as N in page)

			dirAddress = (PAGE_SIZE - ((recordCount + 2) * 4)); //location of the directory (at the bottom of page)

			if (dirAddress - freeSpaceAddress >= totalDataLen)//6b. check if free space is greater than totalData length
            {
				break;
			}
            else
            {
				pageNum++;
				if (pageNum >= fileHandle.getNumberOfPages()) {
					fileHandle.appendPage(page); //appending a new page to the file

					//for a new page, initializing F & N
					unsigned short freeSpaceNew = 0;
					unsigned short recordCountNew = 0;
					memcpy((char *) page + FREE_SPACE_LOCATION, &freeSpaceNew,
							sizeof(unsigned short));
					memcpy((char *) page + RECORD_COUNT_LOCATION,
							&recordCountNew, sizeof(unsigned short));
					break;
				}

			}
		}

	}
    else
    {
		fileHandle.appendPage(page);

		//for a new page, initializing F & N
		unsigned short freeSpaceNew = 0;
		unsigned short recordCountNew = 0;
		memcpy((char *) page + FREE_SPACE_LOCATION, &freeSpaceNew,
				sizeof(unsigned short));
		memcpy((char *) page + RECORD_COUNT_LOCATION, &recordCountNew,
				sizeof(unsigned short));
	}*/


    //6a. to find if page exists:
    unsigned short free_FN = 0;
    unsigned short totalPages= fileHandle.getNumberOfPages();

    if (totalPages > 0)
    {
       while (pageNum < totalPages)
        {
            fileHandle.readPage(pageNum, page);
            unsigned short freeSpaceAddress, recordCount, dirAddress;

            memcpy(&freeSpaceAddress, (char *) page + freeSpaceLocation, sizeof(unsigned short));    //value of the offset till which record is written
            memcpy(&recordCount, (char *) page + recCountLocation, sizeof(unsigned short));  //value of the record count (stored as N in page)
            dirAddress = (PAGE_SIZE - ((recordCount + 2) * 4)); //location of the directory (at the bottom of page)

            unsigned short availableSpace = dirAddress - freeSpaceAddress;

            if (availableSpace >= totalDataLen) //6b. check if free space is greater than totalData length
            {
                break;
            }
            else
            {
                pageNum++;
                if (pageNum >= fileHandle.getNumberOfPages()) {
                    fileHandle.appendPage(page);    //6c appending a new page to the file
                    //for a new page, initializing F & N
                    memcpy((char *) page + freeSpaceLocation, &free_FN, sizeof(unsigned short));
                    memcpy((char *) page + recCountLocation, &free_FN, sizeof(unsigned short));
                    break;
                }
            }
        }

    }
    else //6d when there are no pages in a file
    {
        fileHandle.appendPage(page);
        //for a new page, initializing F & N
        memcpy((char *) page + freeSpaceLocation, &free_FN, sizeof(unsigned short));
        memcpy((char *) page + recCountLocation, &free_FN, sizeof(unsigned short));
    }



	//7. Finding free space in the current page
	unsigned short freeSpaceAdd1 = 0;
	memcpy(&freeSpaceAdd1, (char *) page + freeSpaceLocation,
			sizeof(unsigned short));

	//8.writing record in free space
	memcpy((char *) page + freeSpaceAdd1, record, (size_t) totalDataLen);

	//9.updating location directory
	unsigned short numOfRecords = 0;
	memcpy(&numOfRecords, (char *) page + recCountLocation,
			sizeof(unsigned short));
	numOfRecords++;      //increase 1 record count for the newest record
	memcpy((char *) page + recCountLocation - (numOfRecords * 4),
			&freeSpaceAdd1, sizeof(unsigned short));
	memcpy((char *) page + recCountLocation - (numOfRecords * 4) + 2,
			&totalDataLen, sizeof(unsigned short));

	//10.Update free space and record count (F & N values)
	freeSpaceAdd1 = freeSpaceAdd1 + totalDataLen;

	memcpy((char *) page + freeSpaceLocation, &freeSpaceAdd1,
			sizeof(unsigned short));
	memcpy((char *) page + recCountLocation, &numOfRecords,
			sizeof(unsigned short));

    //11. updating RID
	rid.pageNum = pageNum;
	rid.slotNum = numOfRecords;

	//12. Adding the page to file
	RC rec = fileHandle.writePage(pageNum, page);
	free(page);
    //free(record);
	return rec;

}

RC RecordBasedFileManager::readRecord(FileHandle &fileHandle,
		const vector<Attribute> &recordDescriptor, const RID &rid, void *data)
{
    //initializing offset to "record count location" (N) address

    unsigned short recCountLocation = (PAGE_SIZE - 1 - (2 * sizeof(unsigned short)));

    //cout<<"Inside read record 1";
	unsigned short lenOfRec, locOfRec;
    size_t nullBytes;
    nullBytes = (size_t) ceil((double) recordDescriptor.size() / 8);
    unsigned long addressArrayLen;
    addressArrayLen = (recordDescriptor.size() * 2);
    //reading page
	void *page = malloc(PAGE_SIZE);
	fileHandle.readPage(rid.pageNum, page);

	//finding location and length of record
	memcpy(&locOfRec, (char *) page + recCountLocation - (rid.slotNum * 4),
			sizeof(unsigned short));
	memcpy(&lenOfRec,
			(char *) page + recCountLocation - (rid.slotNum * 4)
					+ sizeof(unsigned short), sizeof(unsigned short));
    //cout<<"Inside read record 1";


	void *record = malloc(lenOfRec);
	memcpy(record, (char *) page + locOfRec, lenOfRec);
	memcpy(data, record, nullBytes);
	memcpy((char *) data + nullBytes,
			(char *) record + nullBytes + addressArrayLen,
           lenOfRec - addressArrayLen - nullBytes);

    //free(record);
    free(page);
	return 0;
}

RC RecordBasedFileManager::printRecord(
		const vector<Attribute> &recordDescriptor, const void *data)
{
    int nullBytesSize = (int) ceil((double) recordDescriptor.size() / 8);
    int ctr = 0;
    bool isNull;
    unsigned char nullIndArray[nullBytesSize];
    memcpy(nullIndArray, data, nullBytesSize);
    int adr = 0;
    adr += nullBytesSize;
    int i=0;


    while(i < recordDescriptor.size())
    {
        cout << recordDescriptor[i].name << " : ";
        isNull = (nullIndArray[ctr / 8]) & (1 << (7 - (ctr % 8))); // finding if null bit
        ctr++;
        if (isNull)
        {
            cout << "NULL ";
            continue;
        }

        if (recordDescriptor[i].type == TypeInt)
        {
            int val = 0;
            memcpy(&val, (char *) data + adr, sizeof(int));
            adr += sizeof(int);
            cout << val << " ";
            i++;

        }
		else if (recordDescriptor[i].type == TypeReal)
        {
            float val = 0;
            memcpy(&val, (char *) data + adr, sizeof(float));
            adr += sizeof(float);
            cout << val << " ";
            i++;

        }
        else
        {
            //deciding length of varchar
            int len = 0;
            memcpy(&len, (char *) data + adr, sizeof(int));
            adr += sizeof(int);

            //actual value of varchar
            char val[len + 1];
            memcpy(val, (char *) data + adr, len);
            val[len] = '\0';
            adr += len;
            cout << val << " ";
            i++;


        }


    }

    cout << endl;
}


