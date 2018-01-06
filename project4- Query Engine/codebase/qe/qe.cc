
#include <cmath>
#include "qe.h"
#include <algorithm>

#define size_pagenum sizeof(PageNum)
#define Bit_size_char sizeof(unsigned char)
#define Bit_size_int sizeof(int)
#define Bit_size_float sizeof(float)
#define Bool_size sizeof(bool)


void
get_attr_val(string attrName, vector<Attribute> recordDescriptor, void *record, void *value, AttrType &attrType)
{

    int offset = 0;
    bool nullBit;
    int nullBitCtr = 0;
    int nullIndSize = (int) ceil((double) recordDescriptor.size() / CHAR_BIT);
    unsigned char nullInd[nullIndSize];


    memcpy(nullInd, record, (size_t) nullIndSize);
    offset = offset + nullIndSize;


    for (Attribute attr : recordDescriptor) {
        nullBit = (bool) ((nullInd[nullBitCtr / CHAR_BIT]) & (1 << (7 - (nullBitCtr % CHAR_BIT))));
        nullBitCtr++;
        bool match = (strcmp(attr.name.c_str(), attrName.c_str()) == 0);
        if (nullBit) {
            if (match) {
                value = NULL;
                return;
            }
            continue;
        }
        attrType = attr.type;
        switch (attr.type) {
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

int cal_offset(int nullIndSize,const vector<Attribute>& recordDescriptor, bool nullBit,
		unsigned char nullInd[],
		int nullBitCtr, int offset, void* record)
{
    nullInd[nullIndSize];
	for (Attribute i : recordDescriptor)
    {
		nullBit = (bool) (((nullInd[nullBitCtr / CHAR_BIT])
				& (1 << (7 - (nullBitCtr % CHAR_BIT)))));
        nullBitCtr++;
		if (nullBit) {
			continue;
		}
		switch (i.type) {
		case TypeInt: {
			offset += Bit_size_int;
			break;
		}
		case TypeReal: {
			offset += Bit_size_float;
			break;
		}
		case TypeVarChar: {
			int length = 0;
			memcpy(&length, (char*) (record) + offset, Bit_size_int);
			offset += Bit_size_int;
			offset += length;
			break;
		}
		}
	}
	return offset;
}

void cal_record_len(vector<Attribute> recordDescriptor, void *record, int &len)
{
    bool nullBit;
    int nullBitCtr = 0;
    int nullIndSize = (int) ceil((double) recordDescriptor.size() / CHAR_BIT);
    unsigned char nullInd[nullIndSize];
    memcpy(nullInd, record, (size_t) nullIndSize);
    int offset = 0;
    offset = offset + nullIndSize;
	offset = cal_offset(nullIndSize,recordDescriptor, nullBit, nullInd,
                        nullBitCtr, offset, record);
    len = offset;
}

void prepare_record(
		unsigned char nullInd[],
		int nullIndSize,
		const vector<Attribute>& leftRecordDescriptor, int leftNullFieldSize,
		const vector<Attribute>& rightRecordDescriptor, int rightNullFieldSize,
		void* finalRec, void* leftRec, void* rightRec)
{
	//prepare output record
    int rightLen;
    int leftLen;
    int offset = 0;
    nullInd[nullIndSize];
	memcpy(finalRec, nullInd, (size_t) (nullIndSize));
	offset += nullIndSize;
    cal_record_len(leftRecordDescriptor, leftRec, leftLen);
	memcpy((char*) (finalRec) + offset, (char*) (leftRec) + leftNullFieldSize,
			(size_t) (leftLen) - leftNullFieldSize);
	offset += leftLen - leftNullFieldSize;
    cal_record_len(rightRecordDescriptor, rightRec, rightLen);
	memcpy((char*) (finalRec) + offset,
			(char*) (rightRec) + rightNullFieldSize,
			(size_t) (rightLen) - rightNullFieldSize);
}

void set_null_ind_arr(int offsetOfLastBit, unsigned char nullInd[],
		int leftNullFieldsSize, unsigned char rightNullsIndicator[],
		int rightNullFieldsSize, int nullIndSize) {
	if (offsetOfLastBit == 0) {
		memcpy(nullInd + leftNullFieldsSize, rightNullsIndicator,
				(size_t) ((rightNullFieldsSize)));
	} else {
		unsigned char curr = nullInd[leftNullFieldsSize - 1];
		for (int i = 0; i < rightNullFieldsSize; i++) {
            nullInd[leftNullFieldsSize - 1 + i] = curr
					& (rightNullsIndicator[i] >> offsetOfLastBit);
            curr = rightNullsIndicator[i] << (CHAR_BIT - offsetOfLastBit);
		}
		if (nullIndSize
				== leftNullFieldsSize + rightNullFieldsSize) {
            nullInd[leftNullFieldsSize - 1] = curr;
		}
	}
}

void last_byt_offset(const vector<Attribute>& leftRecordDescriptor, unsigned char nullInd[], int leftNullFieldSize, unsigned char rightNullInd[], int rightNullFieldSize, int nullIndSize)
{
    rightNullInd[rightNullFieldSize];
    nullInd[nullIndSize];
	int offsetOfLastBit = (int) ((leftRecordDescriptor.size() % CHAR_BIT));
    set_null_ind_arr(offsetOfLastBit, nullInd, leftNullFieldSize,
                     rightNullInd, rightNullFieldSize,nullIndSize);
}

void tuple_join(void *leftRec, vector<Attribute> leftRecordDescriptor, void *rightRec,
                vector<Attribute> rightRecordDescriptor, void *finalRec)
{

    int leftNullFieldSize = (int) ceil((double) leftRecordDescriptor.size() / CHAR_BIT);
    unsigned char leftNullInd[leftNullFieldSize];

    //get null indicator size and prepare null indicatoe array
    int nullIndSize = (int) ceil(
            (double) (leftRecordDescriptor.size() + rightRecordDescriptor.size()) / CHAR_BIT);
    unsigned char nullInd[nullIndSize];


    memcpy(leftNullInd, leftRec, (size_t) leftNullFieldSize);
    memset(nullInd, 0, (size_t) nullIndSize);
    memcpy(nullInd, leftNullInd, (size_t) leftNullFieldSize);

    int rightNullFieldSize = (int) ceil((double) rightRecordDescriptor.size() / CHAR_BIT);
    unsigned char rightNullInd[rightNullFieldSize];
    memcpy(rightNullInd, rightRec, (size_t) rightNullFieldSize);


	last_byt_offset(leftRecordDescriptor, nullInd, leftNullFieldSize,
                    rightNullInd, rightNullFieldSize,
                    nullIndSize);
    //get final record
    prepare_record(nullInd, nullIndSize,
			leftRecordDescriptor, leftNullFieldSize, rightRecordDescriptor,
                   rightNullFieldSize, finalRec, leftRec, rightRec);
}


Filter::Filter(Iterator *input, const Condition &condition) {
    this->input = input;
    this->condition = condition;

    input->getAttributes(recordDescriptor);
}
CompOp compFloat(float x, float y) {
    if (x > y) return GT_OP;
    if (x < y) return LT_OP;
    return EQ_OP;
}

CompOp compStr(const char *str1, const char *str2) {
    if (strcmp(str1, str2) > 0) return GT_OP;
    if (strcmp(str1, str2) < 0) return LT_OP;
    return EQ_OP;
}
CompOp Filter::find_relation(AttrType type, CompOp rel, void* val) {
	switch (type) {
	case TypeInt: {
		int num1, num2;
		memcpy(&num1, (char*) (val), Bit_size_int);
		memcpy(&num2, (char*) (condition.rhsValue.data), Bit_size_int);
        rel = compFloat(num1, num2);
		break;
	}
	case TypeReal: {
		float num1, num2;
		memcpy(&num1, (char*) (val), Bit_size_float);
		memcpy(&num2, (char*) (condition.rhsValue.data), Bit_size_float);
        rel = compFloat(num1, num2);
		break;
	}
	case TypeVarChar: {
		int len = 0;
		memcpy(&len, (char*) (condition.rhsValue.data), Bit_size_int);
		char str2[len + 1];
		memcpy(&str2, (char*) (condition.rhsValue.data) + Bit_size_int,
				(size_t) (len));
		str2[len] = '\0';
		int len2;
		memcpy(&len2, val, Bit_size_int);
		char s1[len2 + 1];
		memcpy(&s1, (char*) (val) + Bit_size_int, len2);
		s1[len2] = '\0';
        rel = compStr(s1, str2);
	}
	}
	return rel;
}

RC Filter::getNextTuple(void *data) {
    if(input->getNextTuple(data) == QE_EOF){
        return QE_EOF;
    }
    void *value = malloc(200);
    AttrType type;
    get_attr_val(condition.lhsAttr, recordDescriptor, data, value, type);

    if (condition.op != NO_OP) {
        CompOp relation;
		relation = find_relation(type, relation, value);
        if (condition.op != relation) {
            if (condition.op == LE_OP && (relation == LT_OP || relation == EQ_OP)) {}
            else if (condition.op == GE_OP && (relation == GT_OP || relation == EQ_OP)) {}
            else if (condition.op == NE_OP && (relation == LT_OP || relation == GT_OP)) {}
            else {
                free(value);
                return getNextTuple(data);
            }
        }
    }
    free(value);
    return 0;
}

void Filter::getAttributes(vector<Attribute> &attrs) const {
    input->getAttributes(attrs);
}

// ... the rest of your implementations go here
Project::Project(Iterator *input, const vector<string> &attrNames) {
    this->input = input;
    this->attrNames = attrNames;
}

//void Project::cal_offset_getnexttuple(int nullIndSize,const vector<Attribute>& recordDescriptor,
//		bool nullBit,
//		unsigned char nullInd[],
//		int nullBitCtr, int outOffset, int offset, void* data,
//		void* readIn) {
//
//}

RC Project::getNextTuple(void *data) {
    void *recIn = malloc(200);
    if(input->getNextTuple(recIn) == QE_EOF){
        free(recIn);
        return QE_EOF;
    }
    vector<Attribute> recordDescriptor;
    input->getAttributes(recordDescriptor);

    bool nullBit;
    int nullBitCtr = 0;
    int offset = 0;
    int outOffset = 0;

    int nullIndSize = (int) ceil((double) recordDescriptor.size() / CHAR_BIT);
    unsigned char nullInd[nullIndSize];
    memcpy(nullInd, recIn, (size_t) nullIndSize);
    offset += nullIndSize;


    int outNullSize = (int) ceil((double) attrNames.size() / CHAR_BIT);
    unsigned char outNullInd[outNullSize];
    memset(outNullInd, 0, (size_t) outNullSize);
    outOffset += outNullSize;
	//cal_offset_getnexttuple(nullIndSize,recordDescriptor, nullBit, nullsInd,nullBitCtr, outOffset, offset, data, recIn);
    nullInd[nullIndSize];
    for (int i=0;i< recordDescriptor.size();i++) {
        vector<string>::iterator itr = find(attrNames.begin(), attrNames.end(),
                                            recordDescriptor[i].name);
        nullBit = (bool) (((nullInd[nullBitCtr / CHAR_BIT])
                           & (1 << (7 - (nullBitCtr % CHAR_BIT)))));
        if (nullBit) {
            nullInd[nullBitCtr / CHAR_BIT] += (1
                    << (7 - (nullBitCtr % CHAR_BIT)));
            continue;
        }
        nullBitCtr++;
        switch (recordDescriptor[i].type) {
            case TypeInt: {
                if (itr != attrNames.end()) {
                    memcpy((char*) (data) + outOffset, (char*) (recIn) + offset,
                           Bit_size_int);
                    outOffset += Bit_size_int;
                }
                offset += Bit_size_int;
                break;
            }
            case TypeReal: {
                if (itr != attrNames.end()) {
                    memcpy((char*) (data) + outOffset, (char*) (recIn) + offset,
                           Bit_size_float);
                    outOffset += Bit_size_float;
                }
                offset += Bit_size_float;
                break;
            }
            case TypeVarChar: {
                int length = 0;
                memcpy(&length, (char*) (recIn) + offset, Bit_size_int);
                offset += Bit_size_int;
                if (itr != attrNames.end()) {
                    memcpy((char*) (data) + outOffset, &length, Bit_size_int);
                    memcpy((char*) (data) + outOffset, (char*) (recIn) + offset,
                           (size_t) (length));
                    outOffset += Bit_size_int + length;
                }
                offset += length;
                break;
            }
        }
    }
    memcpy(data, outNullInd, outNullSize * Bit_size_char);
    free(recIn);
    return 0;
}

void Project::getAttributes(vector<Attribute> &attrs) const {
    vector<Attribute> inputDescriptor;
    input->getAttributes(inputDescriptor);
    for(string str:attrNames){
        for(Attribute attr : inputDescriptor){
            if(strcmp(attr.name.c_str(), str.c_str()) == 0){
                attrs.push_back(attr);
                break;
            }
        }
    }
}

Aggregate::Aggregate(Iterator *input, Attribute aggAttr, AggregateOp op) {
    this->aggAttr = aggAttr;
    this->op = op;
    this->input = input;
    sum = 0;
    count = 0;
    max = 0;
    min = 0;
    eof = false;

    avg = 0;

}

float Aggregate::check_agg_val(float val) {
	switch (op) {
	case MIN: {
		val = min;
		break;
	}
	case MAX: {
		val = max;
		break;
	}
	case COUNT: {
		val = count;
		break;
	}
	case SUM: {
		val = sum;
		break;
	}
	case AVG: {
		val = avg;
		break;
	}
	}
	return val;
}

void Aggregate::cal_min_max_agg(void* value) {
	if (aggAttr.type == TypeInt) {
		int val;
		memcpy(&val, value, Bit_size_int);
		sum += val;
		if (val > max)
			max = val;

		if (val < min)
			min = val;
	} else {
		float val;
		memcpy(&val, value, Bit_size_float);
		sum += val;
		if (val > max)
			max = val;

		if (val < min)
			min = val;
	}
}

RC Aggregate::getNextTuple(void *data) {
    if (eof)
        return QE_EOF;

    void *record = malloc(200);
    vector<Attribute> recordDescriptor;
    input->getAttributes(recordDescriptor);
    while (input->getNextTuple(record) != QE_EOF) {
        void *value = malloc(200);
        AttrType a;
        get_attr_val(aggAttr.name, recordDescriptor, record, value, a);
		cal_min_max_agg(value);
        count++;
        free(value);
    }
    avg = sum / count;
    free(record);
    memset(data, 0, Bit_size_int + 1);
    float val = 0;
	val = check_agg_val(val);
    memcpy((char *) data + 1, &val, Bit_size_float);
    eof = true;
    return 0;
}


void Aggregate::getAttributes(vector<Attribute> &attrs) const {
    string outName = "";
   // check_str_agg_val(name);
    switch (op) {
        case MIN: {
            outName = "MIN";
            break;
        }
        case MAX: {
            outName = "MAX";
            break;
        }
        case COUNT: {
            outName = "COUNT";
            break;
        }
        case SUM: {
            outName = "SUM";
            break;
        }
        case AVG: {
            outName = "AVG";
            break;
        }
    }
    outName = outName + "(" + aggAttr.name + ")";
    Attribute attr1 = aggAttr;
    attr1.name = outName;
    attrs.push_back(attr1);
}

BNLJoin::BNLJoin(Iterator *leftIn, TableScan *rightIn, const Condition &condition, const unsigned numPages) {
    this->left = leftIn;
    this->right = rightIn;
    this->condition = condition;
    this->numPages = numPages;
    buffer = malloc(numPages * PAGE_SIZE);
    rightRec = malloc(200);

    left->getAttributes(leftRecordDescriptor);
    right->getAttributes(rightRecordDescriptor);

    it = matchedRange.second;
    fit = fmatchedRange.second;
    sit = smatchedRange.second;
}

RC BNLJoin::getNextTuple(void *data) {
    if (hashTable.empty()) {
        loadLeftInHashTable();
        if (hashTable.empty()) {
            return QE_EOF;
        }
    }
    if (it != matchedRange.second) {
        joinRecords((char *) buffer + it->second, rightRec, data);
        ++it;
        return 0;
    } else if (fit != fmatchedRange.second) {
        joinRecords((char *) buffer + fit->second, rightRec, data);
        ++fit;
        return 0;
    } else if (sit != smatchedRange.second) {
        joinRecords((char *) buffer + sit->second, rightRec, data);
        ++sit;
        return 0;
    } else {
        if (right->getNextTuple(rightRec) != QE_EOF) {
            int rightLen;
            cal_record_len(rightRecordDescriptor, rightRec, rightLen);

            void *keyVal = malloc(200);
            AttrType keyType;
            get_attr_val(condition.rhsAttr, rightRecordDescriptor, rightRec, keyVal, keyType);
            switch (keyType) {

                case TypeInt: {
                    int key;
                    memcpy(&key, keyVal, Bit_size_int);
                    matchedRange = hashTable.equal_range(key);
                    it = matchedRange.first;
                    if (it == matchedRange.second) return getNextTuple(data);
                    joinRecords((char *) buffer + it->second, rightRec, data);
                    ++it;
                    return 0;
                }
                case TypeReal: {
                    float key;
                    memcpy(&key, keyVal, Bit_size_float);
                    fmatchedRange = fhashTable.equal_range(key);
                    fit = fmatchedRange.first;
                    if (fit == fmatchedRange.second) return getNextTuple(data);
                    joinRecords((char *) buffer + fit->second, rightRec, data);
                    ++fit;
                    return 0;
                }
                case TypeVarChar: {
                    int len;
                    memcpy(&len, keyVal, Bit_size_int);

                    char key[len + 1];
                    memcpy(&key, (char *) keyVal + Bit_size_int, (size_t) len);
                    key[len] = '\0';

                    smatchedRange = shashTable.equal_range(key);
                    sit = smatchedRange.first;
                    if (sit == smatchedRange.second) return getNextTuple(data);
                    joinRecords((char *) buffer + sit->second, rightRec, data);
                    ++sit;
                    return 0;
                }
            }
        } else {
            hashTable.erase(hashTable.begin(), hashTable.end());
            fhashTable.erase(fhashTable.begin(), fhashTable.end());
            shashTable.erase(shashTable.begin(), shashTable.end());
            right->setIterator();
            return getNextTuple(data);
        }
    }
}

int BNLJoin::cal_bufferOffset(AttrType keyType,
		int bufferOffset, int recLen, void* keyVal) {
	switch (keyType) {
	case TypeInt: {
		int key;
		memcpy(&key, keyVal, Bit_size_int);
		hashTable.insert(pair<int, int>(key, bufferOffset));
		bufferOffset += recLen;
		break;
	}
	case TypeReal: {
		float key;
		memcpy(&key, keyVal, Bit_size_float);
		fhashTable.insert(pair<float, int>(key, bufferOffset));
		bufferOffset += recLen;
		break;
	}
	case TypeVarChar: {
		int len;
		memcpy(&len, keyVal, Bit_size_int);
		char key[len + 1];
		memcpy(&key, (char*) (keyVal) + Bit_size_int, (size_t) (len));
		key[len] = '\0';
		shashTable.insert(pair<string, int>(key, bufferOffset));
		bufferOffset += recLen;
		break;
		break;
	}
	}
	return bufferOffset;
}

void BNLJoin::loadLeftInHashTable() {

    int bufferOffset = 0;
    void *recBuf = malloc(200);


    while (left->getNextTuple(recBuf) != QE_EOF) {
        int recLen;
        cal_record_len(leftRecordDescriptor, recBuf, recLen);
        memcpy((char *) buffer + bufferOffset, recBuf, (size_t) recLen);

        void *keyVal = malloc(200);
        AttrType keyType;
        get_attr_val(condition.lhsAttr, leftRecordDescriptor, recBuf, keyVal, keyType);
		bufferOffset = cal_bufferOffset(keyType, bufferOffset,
				recLen, keyVal);
        if (bufferOffset + recLen > numPages * PAGE_SIZE) {
            break;
        }
    }
    free(recBuf);
}

void BNLJoin::joinRecords(void *leftRec, void *rightRec, void *resultRec) {
    tuple_join(leftRec, leftRecordDescriptor, rightRec, rightRecordDescriptor, resultRec);
}

BNLJoin::~BNLJoin() {
    free(buffer);
    free(rightRec);
}

void BNLJoin::getAttributes(vector<Attribute> &attr) const {
    left->getAttributes(attr);

    vector<Attribute> rightAttrs;
    right->getAttributes(rightAttrs);

    copy(begin(rightAttrs), end(rightAttrs), back_inserter(attr));
}


INLJoin::INLJoin(Iterator *leftIn, IndexScan *rightIn, const Condition &condition) {
    this->right = rightIn;
    this->left = leftIn;
    this->condition = condition;

    left->getAttributes(leftRecordDescriptor);
    right->getAttributes(rightRecordDescriptor);
}

RC INLJoin::getNextTuple(void *data) {
    void *leftRec = malloc(200);
    if (left->getNextTuple(leftRec) != QE_EOF) {

        void *value = malloc(200);
        AttrType type;
        get_attr_val(condition.lhsAttr, leftRecordDescriptor, leftRec, value, type);
        right->setIterator(value, value, true, true);
        void *rightRec = malloc(200);
        if (right->getNextTuple(rightRec) != QE_EOF) {
            joinRecords(leftRec, rightRec, data);
            return 0;
        } else {
            return getNextTuple(data);
        }
    }
    return QE_EOF;
}

void INLJoin::joinRecords(void *leftRec, void *rightRec, void *finalRec) {
    tuple_join(leftRec, leftRecordDescriptor, rightRec, rightRecordDescriptor, finalRec);
}

void INLJoin::getAttributes(vector<Attribute> &attr) const {
    left->getAttributes(attr);

    vector<Attribute> rAttr;
    right->getAttributes(rAttr);

    copy(begin(rAttr), end(rAttr), back_inserter(attr));
}


