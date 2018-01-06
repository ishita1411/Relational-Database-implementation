#include "rm.h"
#include "../rbf/rbfm.h"
#include <math.h>
#include <string.h>
#define size_pagenum sizeof(PageNum)
#define Bit_size_char sizeof(unsigned char)
#define Bit_size_int sizeof(int)
#define Bit_size_float sizeof(float)
#define Bool_size sizeof(bool)
#define Bit_char_size sizeof(char)
bool nullBit = false;
int nullIndSize;
int offset = 0;
RelationManager *RelationManager::_rm = 0;

RelationManager *RelationManager::instance() {
    if (!_rm)
        _rm = new RelationManager();

    return _rm;
}

RelationManager::RelationManager() {
    rbfm_obj = RecordBasedFileManager::instance();
    im_obj = IndexManager::instance();
}

RelationManager::~RelationManager() {
}



void RelationManager::prepare_record_columns(int fieldCount,
                                           unsigned char *nullFieldInd, int id, const int columnNameLength,
                                           const string &columnName, const int columnType, const int columnLength,
                                           const int columnPosition, void *buffer, int *recordSizeColumn) {

    offset = 0;
    nullIndSize = (int) ceil((double) fieldCount / CHAR_BIT);


    memcpy((char *) buffer + offset, nullFieldInd,
           nullIndSize);
    offset += nullIndSize;

    //(table-id:int, column-name:varchar(50), column-type:int, column-length:int, column-position:int)
    nullBit = nullFieldInd[0] & (1 << 7);

//	offset = set_offset_prepRecCol(nullBit, offset, id, columnNameLength,
//			columnName, columnType, columnLength, columnPosition, buffer,nullFieldInd);
    if (!nullBit) {
        memcpy((char*) (buffer) + offset, &id, Bit_size_int);
        offset += Bit_size_int;
    }
    nullBit = nullFieldInd[0] & (1 << 6);
    if (!nullBit) {
        memcpy((char*) (buffer) + offset, &columnNameLength, Bit_size_int);
        offset += Bit_size_int;
        memcpy((char*) (buffer) + offset, columnName.c_str(), columnNameLength);
        offset += columnNameLength;
    }
    nullBit = nullFieldInd[0] & (1 << 5);
    if (!nullBit) {
        memcpy((char*) (buffer) + offset, &columnType, Bit_size_int);
        offset += Bit_size_int;
    }
    nullBit = nullFieldInd[0] & (1 << 4);
    if (!nullBit) {
        memcpy((char*) (buffer) + offset, &columnLength, Bit_size_int);
        offset += Bit_size_int;
    }
    nullBit = nullFieldInd[0] & (1 << 3);
    if (!nullBit) {
        memcpy((char*) (buffer) + offset, &columnPosition, Bit_size_int);
        offset += Bit_size_int;
    }
    *recordSizeColumn = offset;
}

//int RelationManager::set_offset_prepRecTab(bool nullBit, int offset,
//		const int tableNameLength, const string& tableName,
//		const int fileNameLength, const string& fileName, void* buffer,
//		unsigned char* nullFieldInd) {
//	if (!nullBit) {
//		memcpy((char*) (buffer) + offset, &tableNameLength, Bit_size_int);
//		offset += Bit_size_int;
//		memcpy((char*) (buffer) + offset, tableName.c_str(), tableNameLength);
//		offset += tableNameLength;
//	}
//	nullBit = nullFieldInd[0] & (1 << 5);
//	if (!nullBit) {
//		memcpy((char*) (buffer) + offset, &fileNameLength, Bit_size_int);
//		offset += Bit_size_int;
//		memcpy((char*) (buffer) + offset, fileName.c_str(), fileNameLength);
//		offset += fileNameLength;
//	}
//	return offset;
//}

void RelationManager::prepare_record_table(int fieldCount,
                                         unsigned char *nullFieldInd, const int tableNameLength,
                                         const int fileNameLength, const string &tableName,
                                         const string &fileName, const int id, void *buffer,
                                         int *recordSizeTable) {

    offset = 0;
    nullIndSize = (int) ceil((double) fieldCount / CHAR_BIT);
    memcpy((char *) buffer + offset, nullFieldInd,
           nullIndSize);
    offset += nullIndSize;

    nullBit = nullFieldInd[0] & (1 << 7);

    if (!nullBit) {
        memcpy((char *) buffer + offset, &id, Bit_size_int);
        offset += Bit_size_int;
    }

    nullBit = nullFieldInd[0] & (1 << 6);
    if (!nullBit) {
        memcpy((char*) (buffer) + offset, &tableNameLength, Bit_size_int);
        offset += Bit_size_int;
        memcpy((char*) (buffer) + offset, tableName.c_str(), tableNameLength);
        offset += tableNameLength;
    }
    nullBit = nullFieldInd[0] & (1 << 5);
    if (!nullBit) {
        memcpy((char*) (buffer) + offset, &fileNameLength, Bit_size_int);
        offset += Bit_size_int;
        memcpy((char*) (buffer) + offset, fileName.c_str(), fileNameLength);
        offset += fileNameLength;
    }
    *recordSizeTable = offset;
}
//int RelationManager::set_offset_prepRecCol(bool nullBit, int offset, int id,
//                                           const int columnNameLength, const string& columnName,
//                                           const int columnType, const int columnLength, const int columnPosition,
//                                           void* buffer, unsigned char* nullFieldInd) {
//    if (!nullBit) {
//        memcpy((char*) (buffer) + offset, &id, Bit_size_int);
//        offset += Bit_size_int;
//    }
//    nullBit = nullFieldInd[0] & (1 << 6);
//    if (!nullBit) {
//        memcpy((char*) (buffer) + offset, &columnNameLength, Bit_size_int);
//        offset += Bit_size_int;
//        memcpy((char*) (buffer) + offset, columnName.c_str(), columnNameLength);
//        offset += columnNameLength;
//    }
//    nullBit = nullFieldInd[0] & (1 << 5);
//    if (!nullBit) {
//        memcpy((char*) (buffer) + offset, &columnType, Bit_size_int);
//        offset += Bit_size_int;
//    }
//    nullBit = nullFieldInd[0] & (1 << 4);
//    if (!nullBit) {
//        memcpy((char*) (buffer) + offset, &columnLength, Bit_size_int);
//        offset += Bit_size_int;
//    }
//    nullBit = nullFieldInd[0] & (1 << 3);
//    if (!nullBit) {
//        memcpy((char*) (buffer) + offset, &columnPosition, Bit_size_int);
//        offset += Bit_size_int;
//    }
//    return offset;
//}
//
//int RelationManager::set_offset_prepRecInd(bool nullBit, int offset,
//		const int indexID, const int tableNameLength, const string& tableName,
//		const int attrNameLength, const string& attrName, void* buffer,
//		unsigned char* nullFieldInd) {
//	if (!nullBit) {
//		memcpy((char*) (buffer) + offset, &indexID, Bit_size_int);
//		offset += Bit_size_int;
//	}
//	nullBit = nullFieldInd[0] & (1 << 6);
//	if (!nullBit) {
//		memcpy((char*) (buffer) + offset, &tableNameLength, Bit_size_int);
//		offset += Bit_size_int;
//		memcpy((char*) (buffer) + offset, tableName.c_str(), tableNameLength);
//		offset += tableNameLength;
//	}
//	nullBit = nullFieldInd[0] & (1 << 5);
//	if (!nullBit) {
//		memcpy((char*) (buffer) + offset, &attrNameLength, Bit_size_int);
//		offset += Bit_size_int;
//		memcpy((char*) (buffer) + offset, attrName.c_str(), attrNameLength);
//		offset += attrNameLength;
//	}
//	return offset;
//}

void RelationManager::prepareRecordIndex(int fieldCount,
                                         unsigned char *nullFieldInd, const int tableNameLength,
                                         const int attrNameLength, const string &tableName,
                                         const string &attrName, const int indexID, void *buffer,
                                         int *recordSizeTable) {
    offset = 0;

    nullIndSize = (int) ceil((double) fieldCount / CHAR_BIT);
    memcpy((char *) buffer + offset, nullFieldInd,
           nullIndSize);
    offset += nullIndSize;

    nullBit = nullFieldInd[0] & (1 << 7);

    if (!nullBit) {
        memcpy((char*) (buffer) + offset, &indexID, Bit_size_int);
        offset += Bit_size_int;
    }
    nullBit = nullFieldInd[0] & (1 << 6);
    if (!nullBit) {
        memcpy((char*) (buffer) + offset, &tableNameLength, Bit_size_int);
        offset += Bit_size_int;
        memcpy((char*) (buffer) + offset, tableName.c_str(), tableNameLength);
        offset += tableNameLength;
    }
    nullBit = nullFieldInd[0] & (1 << 5);
    if (!nullBit) {
        memcpy((char*) (buffer) + offset, &attrNameLength, Bit_size_int);
        offset += Bit_size_int;
        memcpy((char*) (buffer) + offset, attrName.c_str(), attrNameLength);
        offset += attrNameLength;
    }
    *recordSizeTable = offset;
}

void RelationManager::createRecordDescriptorTable(
        vector<Attribute> &recordDescriptor) {

    Attribute attr;
    attr.name = "table-id";
    attr.type = TypeInt;
    attr.length = (AttrLength) 4;
    recordDescriptor.push_back(attr);

    attr.name = "table-name";
    attr.type = TypeVarChar;
    attr.length = (AttrLength) 50;
    recordDescriptor.push_back(attr);

    attr.name = "file-name";
    attr.type = TypeVarChar;
    attr.length = (AttrLength) 50;
    recordDescriptor.push_back(attr);

}

void RelationManager::createRecordDescriptorColumns(
        vector<Attribute> &recordDescriptor) {
    Attribute attr;

    attr.name = "table-id";
    attr.type = TypeInt;
    attr.length = (AttrLength) 4;
    recordDescriptor.push_back(attr);

    attr.name = "column-name";
    attr.type = TypeVarChar;
    attr.length = (AttrLength) 50;
    recordDescriptor.push_back(attr);

    attr.name = "column-type";
    attr.type = TypeInt;
    attr.length = (AttrLength) 4;
    recordDescriptor.push_back(attr);

    attr.name = "column-length";
    attr.type = TypeInt;
    attr.length = (AttrLength) 4;
    recordDescriptor.push_back(attr);

    attr.name = "column-position";
    attr.type = TypeInt;
    attr.length = (AttrLength) 4;
    recordDescriptor.push_back(attr);
}

void RelationManager::createRecordDescriptorIndex(
        vector<Attribute> &recordDescriptor) {
    Attribute attr;

    attr.name = "file-name";
    attr.type = TypeVarChar;
    attr.length = (AttrLength) 50;
    recordDescriptor.push_back(attr);

    attr.name = "attribute-name";
    attr.type = TypeVarChar;
    attr.length = (AttrLength) 50;
    recordDescriptor.push_back(attr);

    attr.name = "table-name";
    attr.type = TypeVarChar;
    attr.length = (AttrLength) 50;
    recordDescriptor.push_back(attr);

}

void RelationManager::insertCatalog(const vector<Attribute> recordDescriptor,
                                    const int tableNameLength,
                                    const int fileNameLength, const string &tableName, const string &fileName,
                                    const int idTable,
                                    int *recordSizeTable, int *recordSizeColumn, RID &ridTable, RID &ridColumn) {
    vector<Attribute> recordDescriptorTable;
    vector<Attribute> recordDescriptorColumn;
    createRecordDescriptorTable(recordDescriptorTable);
    createRecordDescriptorColumns(recordDescriptorColumn);
    FileHandle fileHandleTables;
    FileHandle fileHandleColumns;

    rbfm_obj->openFile(fileNameTable, fileHandleTables);
    rbfm_obj->openFile(fileNameColumn, fileHandleColumns);

    int fieldCountTable = (int) recordDescriptorTable.size();
    int fieldCountColumn = (int) recordDescriptorColumn.size();
    int nullIndSizeT = (int) ceil((double) fieldCountTable / CHAR_BIT);
    int nullIndSizeC =(int) ceil((double) fieldCountColumn / CHAR_BIT);

    unsigned char *nullsIndicatorTable = (unsigned char *) malloc(
            nullIndSizeT);
    memset(nullsIndicatorTable, 0, nullIndSizeT);
    unsigned char *nullsIndicatorColumn = (unsigned char *) malloc(
            nullIndSizeC);
    memset(nullsIndicatorColumn, 0, nullIndSizeC);

    void *recordTable = malloc(100);
    prepare_record_table(recordDescriptorTable.size(), nullsIndicatorTable, tableNameLength, fileNameLength,
                       tableName, fileName, idTable, recordTable, recordSizeTable);
    rbfm_obj->insertRecord(fileHandleTables, recordDescriptorTable, recordTable, ridTable);

    for (unsigned i = 0; i < recordDescriptor.size(); i++) {
        void *recordColumn = malloc(100);
        string columnName = recordDescriptor[i].name;
        const int columnType = recordDescriptor[i].type;
        const int columnLength = recordDescriptor[i].length;
        prepare_record_columns(recordDescriptorColumn.size(), nullsIndicatorColumn,
                             idTable, columnName.length(), columnName, columnType, columnLength, (i + 1), recordColumn,
                             recordSizeColumn);
        rbfm_obj->insertRecord(fileHandleColumns, recordDescriptorColumn, recordColumn, ridColumn);
        free(recordColumn);
    }
    free(recordTable);
    free(nullsIndicatorTable);
    free(nullsIndicatorColumn);
    rbfm_obj->closeFile(fileHandleTables);
    rbfm_obj->closeFile(fileHandleColumns);
}

void RelationManager::updateTableID(int tableID) {
    ofstream tableIDfile;
    tableIDfile.open("tableIDfile.txt");
    tableIDfile << tableID;
    tableIDfile.close();
}

void RelationManager::updateIndexID(int indexID) {
    ofstream indexIDfile;
    indexIDfile.open("indexIDfile.txt");
    indexIDfile << indexID;
    indexIDfile.close();
}

RC RelationManager::getTableID() {
    int tableID;
    ifstream infile;
    infile.open("tableIDfile.txt");

    infile >> tableID;
    return tableID;
}

RC RelationManager::getIndexID() {
    int indexID;
    ifstream infile;
    infile.open("indexIDfile.txt");

    infile >> indexID;
    return indexID;
}

RC RelationManager::createCatalog() {

    //check if files are already created
    ifstream file(fileNameTable);
    if (file.good()) {
        file.close();
        return -1;
    }

    ifstream filen(fileNameColumn);
    if (filen.good()) {
        filen.close();
        return -1;
    }

    ifstream filen1(fileNameIndex);
    if (filen1.good()) {
        filen1.close();
        return -1;
    }


    int recordSizeTable = 0;
    int recordSizeColumn = 0;
    int idTable = 1;

    vector<Attribute> recordDescriptorTable;
    vector<Attribute> recordDescriptorColumn;
    vector<Attribute> recordDescriptorIndex;
    createRecordDescriptorTable(recordDescriptorTable);
    createRecordDescriptorColumns(recordDescriptorColumn);
    createRecordDescriptorIndex(recordDescriptorIndex);

    string tableName = "Tables";
    RID ridTable;
    RID ridColumn;

    rbfm_obj->createFile(fileNameTable);
    rbfm_obj->createFile(fileNameColumn);
    rbfm_obj->createFile(fileNameIndex);
    insertCatalog(recordDescriptorTable,
                  tableName.length(), fileNameTable.length(),
                  tableName, fileNameTable, idTable, &recordSizeTable, &recordSizeColumn, ridTable, ridColumn);
    idTable++;
    string tableName2 = "Columns";
    insertCatalog(recordDescriptorColumn,
                  tableName2.length(), fileNameColumn.length(),
                  tableName2, fileNameColumn, idTable, &recordSizeTable, &recordSizeColumn, ridTable, ridColumn);
    updateTableID(idTable);

    idTable++;
    string tableName3 = "Indices";
    insertCatalog(recordDescriptorIndex,
                  tableName3.length(), fileNameIndex.length(),
                  tableName3, fileNameIndex, idTable, &recordSizeTable, &recordSizeColumn, ridTable, ridColumn);
    updateTableID(idTable);
    return 0;
}

RC RelationManager::deleteCatalog() {
    rbfm_obj->destroyFile(fileNameTable);
    rbfm_obj->destroyFile(fileNameColumn);
    rbfm_obj->destroyFile(fileNameIndex);
    return 0;
}

RC RelationManager::createTable(const string &tableName,
                                const vector<Attribute> &attrs) {

    ifstream file(tableName);
    if (file.good()) {
        file.close();
        return -1;
    }

    int idTable = getTableID() + 1;

    RID ridTable;
    RID ridColumn;
    string fileName = tableName;
    int recordSizeT = 0;
    int recordSizeC = 0;
    FileHandle tableFileHandle;
    rbfm_obj->createFile(tableName);
    insertCatalog(attrs, tableName.length(), fileName.length(),
                  tableName, fileName, idTable, &recordSizeT, &recordSizeC, ridTable, ridColumn);
    updateTableID(idTable);
    return 0;
}

RC RelationManager::deleteTable(const string &tableName) {
    if (tableName.compare(fileNameTable) == 0 || tableName.compare(fileNameColumn) == 0) return -1;
    vector<string> attrTableID = {"table-id"};
    vector<string> attributes = {"column-name"};
    vector<Attribute> recDesTable;
    createRecordDescriptorTable(recDesTable);
    FileHandle fileHandleTable;
    rbfm_obj->openFile(tableName, fileHandleTable);
    void *dataT = malloc(100);
    void *dataC = malloc(100);
    RID ridT;
    RID ridC;
    void *tableID = malloc(10);
    int ID;
    RM_ScanIterator rmsiT;
    RM_ScanIterator rmsiC;

    int vlen = (int) tableName.length();
    void *value = malloc(vlen + Bit_size_int);
    memcpy(value, &vlen, Bit_size_int);
    memcpy((char *) value + Bit_size_int, tableName.c_str(), vlen);

    scan(fileNameTable, "table-name", EQ_OP, value, attrTableID, rmsiT);
    while (rmsiT.getNextTuple(ridT, dataT) != RM_EOF) {
        readAttribute(tableName, ridT, "table-id", tableID);
        memcpy(&ID, (char *) tableID + Bit_char_size, Bit_size_int);
    }
    deleteTuple(tableName, ridT);


    scan(fileNameColumn, "table-id", EQ_OP, &ID, attributes, rmsiC);
    while (rmsiC.getNextTuple(ridC, dataC) != RM_EOF) {
        deleteTuple(tableName, ridC);
    }
    rbfm_obj->destroyFile(tableName);
    free(dataC);
    free(dataT);
    free(tableID);
    free(value);
    return 0;
}

//void* RelationManager::get_attrs_getAttribute(RBFM_ScanIterator rmsiColumn,
//		RID rid, const vector<Attribute>& recordDesColumn,
//		const vector<string>& attributes, void* data,
//		FileHandle& fileHandleColumns, vector<Attribute>& attrs) {
//	void* column = malloc(1000);
//	int varcharLength;
//	int k = 0;
//	Attribute attribute;
//	//table-id:int, column-name:varchar(50), column-type:int, column-length:int, column-position:int
//	while (rmsiColumn.getNextRecord(rid, data) != RBFM_EOF) {
//		//Column Name
//		AttrType type;
//		AttrLength length;
//		rbfm_obj->readAttribute(fileHandleColumns, recordDesColumn, rid,
//				attributes[0], column);
//		memcpy(&varcharLength, (char*) (column) + 1, 4);
//		char name[varcharLength + 1];
//		memcpy(&name, (char*) (column) + 1 + 4, varcharLength);
//		name[varcharLength] = '\0';
//		rbfm_obj->readAttribute(fileHandleColumns, recordDesColumn, rid,
//				attributes[1], column);
//		memcpy(&type, (char*) (column) + 1, 4);
//		rbfm_obj->readAttribute(fileHandleColumns, recordDesColumn, rid,
//				attributes[2], column);
//		memcpy(&length, (char*) (column) + 1, 4);
//		attribute.type = type;
//		attribute.name = name;
//		attribute.length = length;
//		attrs.push_back(attribute);
//		k++;
//	}
//	return column;
//}

RC RelationManager::getAttributes(const string &tableName, vector<Attribute> &attrs) {
    RBFM_ScanIterator rmsiTable;
    RBFM_ScanIterator rmsiColumn;
    void *data = malloc(1000);
    void *tableID = malloc(10);
    int ID;
    RID rid;
    FileHandle fileHandleTables;
    if (rbfm_obj->openFile(fileNameTable, fileHandleTables) != 0) {
        cout << "Opening Tables table failed\n";
        return -1;
    }
    FileHandle fileHandleColumns;
    if (rbfm_obj->openFile(fileNameColumn, fileHandleColumns) != 0) {
        cout << "Opening Columns table failed\n";
        return -1;
    }
    vector<Attribute> recordDesTable;
    vector<Attribute> recordDesColumn;
    createRecordDescriptorTable(recordDesTable);
    createRecordDescriptorColumns(recordDesColumn);
    unsigned char *nullsIndicator = (unsigned char *) malloc(1);
    memset(nullsIndicator, 0, 1);

    vector<string> attrTableID = {"table-id"};

    vector<string> attributes = {"column-name", "column-type", "column-length"};

    int vlen = (int) tableName.length();
    void *value = malloc(vlen + Bit_size_int);
    memcpy(value, &vlen, Bit_size_int);
    memcpy((char *) value + Bit_size_int, tableName.c_str(), vlen);


    rbfm_obj->scan(fileHandleTables, recordDesTable, "table-name", EQ_OP, value, attrTableID, rmsiTable);
    while (rmsiTable.getNextRecord(rid, data) != RBFM_EOF) {
        rbfm_obj->readAttribute(fileHandleTables, recordDesTable, rid, "table-id", tableID);
        memcpy(&ID, (char *) tableID + Bit_char_size, Bit_size_int);
    }
    rbfm_obj->scan(fileHandleColumns, recordDesColumn, "table-id", EQ_OP, &ID, attributes, rmsiColumn);
    void* column = malloc(1000);
    int varcharLength;
    int k = 0;
    Attribute attribute;
    //table-id:int, column-name:varchar(50), column-type:int, column-length:int, column-position:int
    while (rmsiColumn.getNextRecord(rid, data) != RBFM_EOF) {
        //Column Name
        AttrType type;
        AttrLength length;
        rbfm_obj->readAttribute(fileHandleColumns, recordDesColumn, rid,
                                attributes[0], column);
        memcpy(&varcharLength, (char*) (column) + 1, 4);
        char name[varcharLength + 1];
        memcpy(&name, (char*) (column) + 1 + 4, varcharLength);
        name[varcharLength] = '\0';
        rbfm_obj->readAttribute(fileHandleColumns, recordDesColumn, rid,
                                attributes[1], column);
        memcpy(&type, (char*) (column) + 1, 4);
        rbfm_obj->readAttribute(fileHandleColumns, recordDesColumn, rid,
                                attributes[2], column);
        memcpy(&length, (char*) (column) + 1, 4);
        attribute.type = type;
        attribute.name = name;
        attribute.length = length;
        attrs.push_back(attribute);
        k++;
    }
    rbfm_obj->closeFile(fileHandleColumns);
    rbfm_obj->closeFile(fileHandleTables);
    free(column);
    free(data);
    free(tableID);
    free(nullsIndicator);
    free(value);

    return 0;
}

RC RelationManager::insertTuple(const string &tableName, const void *data,
                                RID &rid) {
    vector<Attribute> recDes;
    getAttributes(tableName, recDes);
    FileHandle fileHandleT;
    RC rc;
    if (rbfm_obj->openFile(tableName, fileHandleT) == 0) {
        rc = rbfm_obj->insertRecord(fileHandleT, recDes, data, rid);
        insertIndexEntries(tableName, data, rid);
        rbfm_obj->closeFile(fileHandleT);
    } else {
        cout << "open failed\n";
        rc = -1;
    }
    return rc;
}

RC RelationManager::deleteTuple(const string &tableName, const RID &rid) {
    vector<Attribute> recDes;
    getAttributes(tableName, recDes);
    FileHandle fileHandleT;
    RC code;
    if (rbfm_obj->openFile(tableName, fileHandleT) == 0) {
        void *data = malloc(200);
        readTuple(tableName, rid, data);
        code = rbfm_obj->deleteRecord(fileHandleT, recDes, rid);
        deleteIndexEntries(tableName, data, rid);
        rbfm_obj->closeFile(fileHandleT);
        free(data);
    } else {
        code = -1;
    }
    return code;
}

RC RelationManager::updateTuple(const string &tableName, const void *data,
                                const RID &rid) {
    vector<Attribute> recDes;
    getAttributes(tableName, recDes);
    FileHandle fileHandleT;
    RC code;

    if (rbfm_obj->openFile(tableName, fileHandleT) == 0) {
        code = rbfm_obj->updateRecord(fileHandleT, recDes, data, rid);
        rbfm_obj->closeFile(fileHandleT);
    } else {
        code = -1;
    }
    return code;
}

RC RelationManager::readTuple(const string &tableName, const RID &rid,
                              void *data) {
    vector<Attribute> recDes;
    getAttributes(tableName, recDes);
    FileHandle fileHandleT;
    RC code;
    if (rbfm_obj->openFile(tableName, fileHandleT) == 0) {
        code = rbfm_obj->readRecord(fileHandleT, recDes, rid, data);
        rbfm_obj->closeFile(fileHandleT);
    } else {
        code = -1;
    }
    return code;
}

RC RelationManager::printTuple(const vector<Attribute> &attrs,
                               const void *data) {
    rbfm_obj->printRecord(attrs, data);
    return 0;
}

RC RelationManager::readAttribute(const string &tableName, const RID &rid,
                                  const string &attributeName, void *data) {
    vector<Attribute> recordDescriptor;
    getAttributes(tableName, recordDescriptor);
    FileHandle fileTable;
    RC rc;
    if (rbfm_obj->openFile(tableName, fileTable) == 0) {
        rc = rbfm_obj->readAttribute(fileTable, recordDescriptor, rid, attributeName, data);
        rbfm_obj->closeFile(fileTable);
    } else {
        rc = -1;
    }
    return rc;
}

RC RelationManager::scan(const string &tableName,
                         const string &conditionAttribute, const CompOp compOp,
                         const void *value, const vector<string> &attributeNames,
                         RM_ScanIterator &rm_ScanIterator) {
    RBFM_ScanIterator rbfmsi;
    vector<Attribute> recordDescriptor;
    getAttributes(tableName, recordDescriptor);
    FileHandle fileTable;
    RC rc;
    if (rbfm_obj->openFile(tableName, fileTable) == 0) {
        rc = rbfm_obj->scan(fileTable, recordDescriptor, conditionAttribute, compOp, value, attributeNames, rbfmsi);
        rm_ScanIterator.initialize(rbfmsi);
    } else {
        rc = -1;
    }
    return rc;
}


RC RelationManager::dropAttribute(const string &tableName,
                                  const string &attributeName) {
    return -1;
}


RC RelationManager::addAttribute(const string &tableName,
                                 const Attribute &attr) {
    return -1;
}

RC RelationManager::createIndex(const string &tableName, const string &attributeName)
{
    RID rid;
    string indexFileName = tableName + "_" + attributeName;
    im_obj->createFile(indexFileName);
    IXFileHandle ixfileHandleAttribute;
    im_obj->openFile(indexFileName, ixfileHandleAttribute);
    vector<string> attributes;
    attributes.push_back(attributeName);
    RM_ScanIterator *iter = new RM_ScanIterator();
    RC rc = scan(tableName, "", NO_OP, NULL, attributes, *iter);

    void *returnedData = malloc(200);
    Attribute attrInsert;
    vector<Attribute> tableAttr;
    getAttributes(tableName, tableAttr);
    for (int i = 0; i < tableAttr.size(); i++) {
        if (tableAttr[i].name == attributeName) {
            attrInsert = tableAttr[i];
            break;
        }
    }

    while (iter->getNextTuple(rid, returnedData) != RM_EOF) {
        char *key = (char *) returnedData + 1;
        im_obj->insertEntry(ixfileHandleAttribute, attrInsert, key, rid);
    }
    im_obj->closeFile(ixfileHandleAttribute);
    free(returnedData);
    return insertIndexInCatalog(tableName, attributeName);;
}

RC RelationManager::destroyIndex(const string &tableName, const string &attributeName) {
    string indexFileName = tableName + "_" + attributeName;
    im_obj->destroyFile(indexFileName);

    string fileName = tableName + "_" + attributeName;
    int len = (int) fileName.length();
    void *data = malloc((size_t) (len + 4));
    memcpy(data, &len, Bit_size_int);
    memcpy((char *) data + Bit_size_int, fileName.c_str(), (size_t) len);
    vector<string> recordDescriptorIndex;
    recordDescriptorIndex.push_back("file_name");
    RM_ScanIterator rmsi;
    scan(fileNameIndex, "file-name", EQ_OP, data, recordDescriptorIndex, rmsi);

    RID rid;
    RC deleteRC = -1;
    while (rmsi.getNextTuple(rid, data) != RM_EOF) {
        deleteRC = deleteTuple(fileNameIndex, rid);
    }

    free(data);
    return deleteRC;
}

RC RelationManager::indexScan(const string &tableName, const string &attributeName, const void *lowKey,
                              const void *highKey, bool lowKeyInclusive, bool highKeyInclusive,
                              RM_IndexScanIterator &rm_IndexScanIterator) {
    IX_ScanIterator ix_scanIterator;
    vector<Attribute> recordDescriptor;
    Attribute indexAttribute;
    getAttributes(tableName, recordDescriptor);
    for (int i = 0; i < recordDescriptor.size(); i++) {
        if (strcmp(recordDescriptor[i].name.c_str(), attributeName.c_str()) == 0) {
            indexAttribute = recordDescriptor[i];
            break;
        }
    }
    IXFileHandle fileIndex;
    RC rc;
    if (im_obj->openFile(tableName + "_" + attributeName, fileIndex) == 0) {
        rc = im_obj->scan(fileIndex, indexAttribute, lowKey, highKey, lowKeyInclusive, highKeyInclusive,
                          ix_scanIterator);
        rm_IndexScanIterator.initialize(ix_scanIterator);
    } else {
        rc = -1;
    }
    return rc;
}

//void RelationManager::set_offset_InsertIndCatalog(
//		unsigned char nullBitsIndicator, int len1, const string& fileName,
//		int len2, const string& attributeName, int len3,
//		const string& tableName, void* data) {
//	offset = 0;
//	memcpy(data, &nullBitsIndicator, Bit_size_char);
//	offset += Bit_size_char;
//	memcpy((char*) (data) + offset, &len1, Bit_size_int);
//	offset += Bit_size_int;
//	memcpy((char*) (data) + offset, fileName.c_str(), (size_t) (len1));
//	offset += len1;
//	memcpy((char*) (data) + offset, &len2, Bit_size_int);
//	offset += Bit_size_int;
//	memcpy((char*) (data) + offset, attributeName.c_str(), (size_t) (len2));
//	offset += len2;
//	memcpy((char*) (data) + offset, &len3, Bit_size_int);
//	offset += Bit_size_int;
//	memcpy((char*) (data) + offset, tableName.c_str(), (size_t) (len3));
//	offset += len3;
//}

RC RelationManager::insertIndexInCatalog(const string tableName, const string attributeName) {
    unsigned char nullBitsIndicator;
    memset(&nullBitsIndicator, 0, Bit_size_char);

    string fileName = tableName + "_" + attributeName;
    int len1 = (int) fileName.length(), len2 = (int) attributeName.length(), len3 = (int) tableName.length();
    int recordSize = 1 + Bit_size_int * 3 + len1 + len2 + len3;
    void *data = malloc((size_t) recordSize);

    offset = 0;
    memcpy(data, &nullBitsIndicator, Bit_size_char);
    offset += Bit_size_char;
    memcpy((char*) (data) + offset, &len1, Bit_size_int);
    offset += Bit_size_int;
    memcpy((char*) (data) + offset, fileName.c_str(), (size_t) (len1));
    offset += len1;
    memcpy((char*) (data) + offset, &len2, Bit_size_int);
    offset += Bit_size_int;
    memcpy((char*) (data) + offset, attributeName.c_str(), (size_t) (len2));
    offset += len2;
    memcpy((char*) (data) + offset, &len3, Bit_size_int);
    offset += Bit_size_int;
    memcpy((char*) (data) + offset, tableName.c_str(), (size_t) (len3));
    offset += len3;
    RID rid;
    RC rc = insertTuple(fileNameIndex, data, rid);
    free(data);
    return rc;


}

size_t RelationManager::getAttrLen(const Attribute &attribute, const void *key) {
    return (size_t) im_obj->getKeyLen(attribute, key);
}


void getAttributeValue(string attrName, vector<Attribute> recordDescriptor, const void *record, void *value,
                       AttrType &attrType) {

    offset = 0;

    nullIndSize = (int) ceil((double) recordDescriptor.size() / CHAR_BIT);
    unsigned char nullsIndicator[nullIndSize];
    memcpy(nullsIndicator, record, (size_t) nullIndSize);
    offset += nullIndSize;

    int nullBitCounter = 0;
    for (Attribute i : recordDescriptor) {
        nullBit = (bool) ((nullsIndicator[nullBitCounter / CHAR_BIT]) & (1 << (7 - (nullBitCounter % CHAR_BIT))));
        nullBitCounter++;
        bool match = (strcmp(i.name.c_str(), attrName.c_str()) == 0);
        if (nullBit) {
            if (match) {
                value = NULL;
                return;
            }
            continue;
        }
        attrType = i.type;
        switch (i.type) {
            case TypeInt: {
                if (match) {
                    memcpy(value, (char *) record + offset, Bit_size_int);
                    return;
                }
                offset += Bit_size_int;
                break;
            }
            case TypeReal: {
                if (match) {
                    memcpy(value, (char *) record + offset, Bit_size_int);
                    return;
                }
                offset += Bit_size_float;
                break;
            }
            case TypeVarChar: {
                //Get length of varchar
                int length = 0;
                memcpy(&length, (char *) record + offset, Bit_size_int);

                //Get actual value of varchar
                if (match) {
                    memcpy(value, (char *) record + offset, (size_t) (length + Bit_size_int));
                    return;
                }
                offset += Bit_size_int;
                offset += length;
                break;
            }
        }
    }
}
//
//void* RelationManager::Traverse_tuple_InsetEnt(
//		const vector<Attribute>& recordDescriptor, const void* record,
//		const string& tableName, RID rid, RM_ScanIterator& rm_scanIterator) {
//	RID trid;
//	void* data = malloc(200);
//	while (rm_scanIterator.getNextTuple(trid, data) != RM_EOF) {
//		int attrNameLen;
//		memcpy(&attrNameLen, (char*) (data) + Bit_char_size, Bit_size_int);
//		char attrName[attrNameLen + 1];
//		memcpy(attrName, (char*) (data) + Bit_char_size + Bit_size_int,
//				(size_t) (attrNameLen));
//		attrName[attrNameLen] = '\0';
//		for (Attribute i : recordDescriptor) {
//			if (strcmp(i.name.c_str(), attrName) == 0) {
//				AttrType type;
//				void* attrVal = malloc(200);
//				getAttributeValue(attrName, recordDescriptor, record, attrVal,
//						type);
//				string fileName = tableName + "_" + attrName;
//				IXFileHandle ixFileHandle;
//				im_obj->openFile(fileName, ixFileHandle);
//				im_obj->insertEntry(ixFileHandle, i, attrVal, rid);
//				im_obj->closeFile(ixFileHandle);
//				free(attrVal);
//			}
//		}
//	}
//	return data;
//}

void RelationManager::insertIndexEntries(string tableName, const void *record, RID rid) {
    //insert in index if it exists


    vector<Attribute> recordDescriptor;
    if (getAttributes(tableName, recordDescriptor) != 0) {
        cout << "get Attributes failed\n";
        return;
    }

    vector<string> attrs = {"attribute-name"};
    RM_ScanIterator rm_scanIterator;

    int len = (int) tableName.length();
    void *value = malloc(Bit_size_int + len);
    memcpy(value, &len, Bit_size_int);
    memcpy((char *) value + Bit_size_int, tableName.c_str(), (size_t) len);
    RC rc = scan(fileNameIndex, "table-name", EQ_OP, value, attrs, rm_scanIterator);
    RID trid;
    void* data = malloc(200);
    while (rm_scanIterator.getNextTuple(trid, data) != RM_EOF) {
        int attrNameLen;
        memcpy(&attrNameLen, (char*) (data) + Bit_char_size, Bit_size_int);
        char attrName[attrNameLen + 1];
        memcpy(attrName, (char*) (data) + Bit_char_size + Bit_size_int,
               (size_t) (attrNameLen));
        attrName[attrNameLen] = '\0';
        for (Attribute i : recordDescriptor) {
            if (strcmp(i.name.c_str(), attrName) == 0) {
                AttrType type;
                void* attrVal = malloc(200);
                getAttributeValue(attrName, recordDescriptor, record, attrVal,
                                  type);
                string fileName = tableName + "_" + attrName;
                IXFileHandle ixFileHandle;
                im_obj->openFile(fileName, ixFileHandle);
                im_obj->insertEntry(ixFileHandle, i, attrVal, rid);
                im_obj->closeFile(ixFileHandle);
                free(attrVal);
            }
        }
    }
    rm_scanIterator.close();
    free(data);
    free(value);
}

//void RelationManager::Traverse_tuple_deleteInd(RID trid,
//		const vector<Attribute>& recordDescriptor, const string& tableName,
//		const RID& rid, RM_ScanIterator& rm_scanIterator, void* data,
//		void* record) {
//	while (rm_scanIterator.getNextTuple(trid, data) != RM_EOF) {
//		int attrNameLen;
//		memcpy(&attrNameLen, (char*) (data) + Bit_char_size, Bit_size_int);
//		char attrName[attrNameLen + 1];
//		memcpy(attrName, (char*) (data) + Bit_char_size + Bit_size_int,
//				(size_t) (attrNameLen));
//		attrName[attrNameLen] = '\0';
//		for (Attribute i : recordDescriptor) {
//			if (strcmp(i.name.c_str(), attrName) == 0) {
//				AttrType type;
//				void* attrVal = malloc(200);
//				getAttributeValue(attrName, recordDescriptor, record, attrVal,
//						type);
//				string fileName = tableName + "_" + attrName;
//				IXFileHandle ixFileHandle;
//				im_obj->openFile(fileName, ixFileHandle);
//				im_obj->deleteEntry(ixFileHandle, i, attrVal, rid);
//				im_obj->closeFile(ixFileHandle);
//				free(attrVal);
//			}
//		}
//	}
//}

void RelationManager::deleteIndexEntries(const string &tableName, void *record, const RID &rid) {
//insert in index if it exists
    int len = (int) tableName.length();
    void *value = malloc(Bit_size_int + len);
    memcpy(value, &len, Bit_size_int);
    memcpy((char *) value + Bit_size_int, tableName.c_str(), (size_t) len);

    vector<Attribute> recordDescriptor;
    getAttributes(tableName, recordDescriptor);

    vector<string> attrs = {"attribute-name"};
    RM_ScanIterator rm_scanIterator;
    scan(fileNameIndex, "table-name", EQ_OP, value, attrs, rm_scanIterator);

    RID trid;
    void *data = malloc(200);
    while (rm_scanIterator.getNextTuple(trid, data) != RM_EOF) {
        int attrNameLen;
        memcpy(&attrNameLen, (char*) (data) + Bit_char_size, Bit_size_int);
        char attrName[attrNameLen + 1];
        memcpy(attrName, (char*) (data) + Bit_char_size + Bit_size_int,
               (size_t) (attrNameLen));
        attrName[attrNameLen] = '\0';
        for (Attribute i : recordDescriptor) {
            if (strcmp(i.name.c_str(), attrName) == 0) {
                AttrType type;
                void* attrVal = malloc(200);
                getAttributeValue(attrName, recordDescriptor, record, attrVal,
                                  type);
                string fileName = tableName + "_" + attrName;
                IXFileHandle ixFileHandle;
                im_obj->openFile(fileName, ixFileHandle);
                im_obj->deleteEntry(ixFileHandle, i, attrVal, rid);
                im_obj->closeFile(ixFileHandle);
                free(attrVal);
            }
        }
    }
    rm_scanIterator.close();
    free(data);
    free(value);
}


/***********************************************************************************************************************
 * RM Scan Iterator
 **********************************************************************************************************************/

void RM_ScanIterator::initialize(RBFM_ScanIterator scanIterator) {
    _scanIterator = scanIterator;
}

RC RM_ScanIterator::getNextTuple(RID &rid, void *data) {
    RC rc = _scanIterator.getNextRecord(rid, data);
    return (rc == RBFM_EOF) ? RM_EOF : rc;
}

RC RM_ScanIterator::close() {
    return _scanIterator.close();
}

RC RM_IndexScanIterator::getNextEntry(RID &rid, void *key) {
    RC rc = ix_scanIterator.getNextEntry(rid, key);
    return (rc == IX_EOF) ? RM_EOF : rc;
}

RC RM_IndexScanIterator::close() {
    return ix_scanIterator.close();
}

void RM_IndexScanIterator::initialize(IX_ScanIterator scanIterator) {
    ix_scanIterator = scanIterator;
}
