#include "IndexSystem.h"
#include <cstring>

bool IndexHandle::isSmaller(const char *data1, const char *data2, const RID &rid1, const RID &rid2) {
    int offset = 0;
    for (int i = 0; i < _header._attrNum; i++) {
        switch (_attrTypes[i]) {
            case INTEGER:
                int int_a, int_b;
                memcpy(&int_a, data1 + offset, _attrLens[i]);
                memcpy(&int_b, data2 + offset, _attrLens[i]);
                if (int_a < int_b) return true;
                if (int_a > int_b) return false;
                break;
            case FLOAT:
                float float_a, float_b;
                memcpy(&float_a, data1 + offset, _attrLens[i]);
                memcpy(&float_b, data2 + offset, _attrLens[i]);
                if (float_a < float_b) return true;
                if (float_a > float_b) return false;
                break;
            case STRING:
                int cmp;
                cmp = memcmp(data1 + offset, data2 + offset, _attrLens[i]);
                if (cmp < 0) return true;
                if (cmp > 0) return false;
                break;
            default:
                break;
        }
        offset += _attrLens[i];
    }
    if (rid1.getPageNum() < rid2.getPageNum()) return true;
    if (rid2.getPageNum() > rid2.getPageNum()) return false;
    return rid1.getSlotNum() < rid2.getSlotNum();
}

Node *IndexHandle::getNodeById(int id, bool isNew) const {
    Node *node = new Node();
    node->_start = _bufPageManager->getPage(_fileID, id, node->_index);
    _bufPageManager->access(node->_index);
    memcpy(&node->_isLeaf, node->_start, 6 * sizeof(int));
    if (isNew) {
        memset(node->_start, 0, PAGE_SIZE);
        node->_prev = node->_next = 0;
    }
    node->_key = (char *) node->_start + _header._keyStart;
    node->_child = (int *) ((char *) node->_start + _header._childStart);
    node->_rid = (RID *) ((char *) node->_start + _header._ridStart);
    return node;
}

void IndexHandle::refreshNode(Node *node) const {
    memcpy(node->_start, &node->_isLeaf, 6 * sizeof(int));
    _bufPageManager->markDirty(node->_index);
    _bufPageManager->writeBack(node->_index);
}

void IndexHandle::refreshHeader() const {
    int index;
    BufType b = _bufPageManager->getPage(_fileID, 0, index);
    memcpy(b, &_header, sizeof(IndexHeader));
    _bufPageManager->markDirty(index);
    _bufPageManager->writeBack(index);
}

void IndexHandle::refreshTree(Node *&node, int id) const {
    while (id != _header._root) {
        Node *parentNode = getNodeById(node->_parent);
        int i = 0;
        while (parentNode->_child[i] != id) i++;
        memcpy(parentNode->_key + i * _header._attrLen, node->_key, _header._attrLen);
        parentNode->_rid[i] = node->_rid[0];
        refreshNode(parentNode);
        id = node->_parent;
        delete node;
        node = parentNode;
    }
}

IndexHandle::IndexHandle(BufPageManager *bufPageManager, int fileID) {
    _bufPageManager = bufPageManager;
    _fileID = fileID;
    int index;
    BufType b = _bufPageManager->getPage(_fileID, 0, index);
    _bufPageManager->access(index);
    memcpy(&_header, b, sizeof(IndexHeader));
    for (int i = 0; i < _header._attrNum; i++) {
        _attrTypes.push_back(((AttrType *) ((char *) b + sizeof(IndexHeader)))[i]);
        _attrLens.push_back(((int *) ((char *) b + sizeof(IndexHeader) + _header._attrNum * sizeof(AttrType)))[i]);
    }
}

bool IndexHandle::insertEntry(BufType data, const RID &rid, bool isUnique, bool check) {
    int id = _header._root;
    //??????????????????????????????????????????
    if (isUnique) {
        Node *node = getNodeById(id);
        RID r(-1, -1);
        //???????????????????????????????????????
        while (!node->_isLeaf) {
            for (int i = node->_keyNum - 1; i >= 0; i--) {
                //key < data??????data??????
                if (i == 0 || isSmaller(node->_key + i * _header._attrLen, (char *)data, node->_rid[i], r)) {
                    id = node->_child[i];
                    node = getNodeById(id);
                    break;
                }
            }
        }
        int pos = 0;
        while (pos < node->_keyNum) {
            //key > data
            if (isSmaller((char *)data, node->_key + pos * _header._attrLen, r, node->_rid[pos])) {
                break;
            }
            pos++;
        }
        //?????????????????????????????????????????????
        if (pos < node->_keyNum) {
            if (memcmp(data, node->_key + pos * _header._attrLen, _header._attrLen) == 0) {
                delete node;
                return false;
            }
        } else if (node->_next != 0) {
            pos = 0;
            Node *n = getNodeById(node->_next);
            if (memcmp(data, n->_key + pos * _header._attrLen, _header._attrLen) == 0) {
                delete node;
                delete n;
                return false;
            }
            delete n;
        }
        delete node;
        if (check) return true;
    }
    Node *node = getNodeById(id);
    //???????????????????????????????????????
    while (!node->_isLeaf) {
        for (int i = node->_keyNum - 1; i >= 0; i--) {
            //key < data??????data??????
            if (i == 0 || isSmaller(node->_key + i * _header._attrLen, (char *)data, node->_rid[i], rid)) {
                id = node->_child[i];
                node = getNodeById(id);
                break;
            }
        }
    }
    int pos = node->_keyNum - 1;
    while (pos >= 0) {
        //???????????????key < data??????????????????
        if (isSmaller(node->_key + pos * _header._attrLen, (char *)data, node->_rid[pos], rid)) {
            memcpy(node->_key + (pos + 1) * _header._attrLen, data, _header._attrLen);
            node->_rid[pos + 1] = rid;
            break;
        }
        //???????????????????????????
        memcpy(node->_key + (pos + 1) * _header._attrLen, node->_key + pos * _header._attrLen, _header._attrLen);
        node->_rid[pos + 1] = node->_rid[pos];
        pos--;
    }
    //??????data??????????????????????????????
    if (pos == -1) {
        memcpy(node->_key, data, _header._attrLen);
        node->_rid[0] = rid;
    }
    node->_keyNum++;
    bool overflow = false;
    //????????????
    if (node->_keyNum == _header._maxChildNum) {
        overflow = true;
        //????????????????????????
        while (node->_keyNum == _header._maxChildNum) {
            int parent = node->_parent;
            Node *parentNode;
            if (parent != 0) parentNode = getNodeById(parent);
            //???????????????????????????
            else {
                //???????????????????????????????????????????????????
                if (_header._firstEmptyPage != 0) {
                    parentNode = getNodeById(_header._firstEmptyPage, true);
                    parent = _header._firstEmptyPage;
                    _header._firstEmptyPage = parentNode->_nextEmptyPage;
                } else {
                    _header._pageNumber++;
                    parent = _header._pageNumber;
                    parentNode = getNodeById(_header._pageNumber, true);
                }
                //?????????????????????
                parentNode->_isLeaf = false;
                parentNode->_keyNum = 1;
                parentNode->_parent = 0;
                parentNode->_nextEmptyPage = 0;
                memcpy(parentNode->_key, node->_key, _header._attrLen);
                parentNode->_child[0] = id;
                parentNode->_rid[0] = node->_rid[0];
                node->_parent = parent;
                //???????????????id
                _header._root = parent;
            }
            //???????????????????????????????????????
            int i = 0;
            while (parentNode->_child[i] != id) i++;
            //????????????????????????????????????
            for (int j = parentNode->_keyNum - 1; j > i; j--) {
                memcpy(parentNode->_key + (j + 1) * _header._attrLen, parentNode->_key + j * _header._attrLen, _header._attrLen);
                parentNode->_child[j + 1] = parentNode->_child[j];
                parentNode->_rid[j + 1] = parentNode->_rid[j];
            }
            parentNode->_keyNum++;
            //???????????????
            Node *newNode;
            int newID;
            //??????????????????????????????
            if (_header._firstEmptyPage != 0) {
                newID = _header._firstEmptyPage;
                newNode = getNodeById(_header._firstEmptyPage, true);
                parentNode->_child[i + 1] = newID;
                _header._firstEmptyPage = newNode->_nextEmptyPage;
            } else {
                _header._pageNumber++;
                newID = _header._pageNumber;
                newNode = getNodeById(_header._pageNumber, true);
                parentNode->_child[i + 1] = newID;
            }
            //????????????????????????????????????
            newNode->_isLeaf = node->_isLeaf;
            newNode->_keyNum = node->_keyNum - node->_keyNum / 2;
            newNode->_parent = node->_parent;
            newNode->_nextEmptyPage = 0;
            node->_keyNum /= 2;
            //????????????????????????????????????????????????
            for (int j = 0; j < newNode->_keyNum; j++) {
                memcpy(newNode->_key + j * _header._attrLen, node->_key + (node->_keyNum + j) * _header._attrLen, _header._attrLen);
                newNode->_child[j] = node->_child[node->_keyNum + j];
                //??????????????????????????????????????????id
                if (!newNode->_isLeaf && newNode->_child[j] != 0) {
                    Node *childNode = getNodeById(newNode->_child[j]);
                    childNode->_parent = newID;
                    refreshNode(childNode);
                    delete childNode;
                }
                newNode->_rid[j] = node->_rid[node->_keyNum + j];
            }
            //?????????????????????????????????????????????????????????
            if (newNode->_isLeaf) {
                newNode->_prev = id;
                newNode->_next = node->_next;
                //??????????????????????????????????????????
                if (node->_next != 0) {
                    Node *brotherNode = getNodeById(node->_next);
                    brotherNode->_prev = newID;
                    refreshNode(brotherNode);
                    delete brotherNode;
                }
                node->_next = newID;
            }
            //????????????????????????
            memcpy(parentNode->_key + i * _header._attrLen, node->_key, _header._attrLen);
            memcpy(parentNode->_key + (i + 1) * _header._attrLen, newNode->_key, _header._attrLen);
            parentNode->_rid[i] = node->_rid[0];
            parentNode->_rid[i + 1] = newNode->_rid[0];
            refreshNode(node);
            refreshNode(newNode);
            delete node;
            delete newNode;
            //????????????????????????????????????
            id = parent;
            node = parentNode;
        }
    }
    refreshNode(node);
    //?????????????????????????????????????????????
    if (overflow) refreshHeader();
    refreshTree(node, id);
    delete node;
    return true;
}

bool IndexHandle::deleteEntry(BufType data, const RID &rid) {
    int id = _header._root;
    Node *node = getNodeById(id);
    //???????????????????????????????????????
    while (!node->_isLeaf) {
        for (int i = node->_keyNum - 1; i >= 0; i--) {
            //key < data??????data??????
            if (i == 0 || !isSmaller((char *)data, node->_key + i * _header._attrLen, rid, node->_rid[i])) {
                id = node->_child[i];
                node = getNodeById(id);
                break;
            }
        }
    }
    int pos = node->_keyNum - 1;
    while (pos >= 0) {
        //??????????????????
        if (!isSmaller((char *)data, node->_key + pos * _header._attrLen, rid, node->_rid[pos])) {
            break;
        }
        pos--;
    }
    //???????????????????????????????????????false
    if (pos == -1) {
        delete node;
        return false;
    }
    node->_keyNum--;
    //???????????????????????????
    while (pos < node->_keyNum) {
        memcpy(node->_key + pos * _header._attrLen, node->_key + (pos + 1) * _header._attrLen, _header._attrLen);
        node->_rid[pos] = node->_rid[pos + 1];
        pos++;
    }
    bool underflow = false;
    //????????????
    if (node->_keyNum == 0) {
        underflow = true;
        //?????????????????????
        if (node->_prev) {
            Node *prev = getNodeById(node->_prev);
            prev->_next = node->_next;
            refreshNode(prev);
            delete prev;
        }
        if (node->_next) {
            Node *next = getNodeById(node->_next);
            next->_prev = node->_prev;
            refreshNode(next);
            delete next;
        }
        //????????????????????????
        while (node->_keyNum == 0) {
            //??????????????????
            if (id == _header._root) {
                if (node->_keyNum == 0) node->_isLeaf = true;
                break;
            }
            int parent = node->_parent;
            Node *parentNode = getNodeById(parent);
            //???????????????????????????????????????
            int i = 0;
            while (parentNode->_child[i] != id) i++;
            parentNode->_keyNum--;
            //?????????????????????????????????
            while (i < parentNode->_keyNum) {
                memcpy(parentNode->_key + i * _header._attrLen, parentNode->_key + (i + 1) * _header._attrLen, _header._attrLen);
                parentNode->_child[i] = parentNode->_child[i + 1];
                parentNode->_rid[i] = parentNode->_rid[i + 1];
                i++;
            }
            //????????????
            node->_nextEmptyPage = _header._firstEmptyPage;
            _header._firstEmptyPage = id;
            refreshNode(node);
            delete node;
            node = parentNode;
            //????????????????????????????????????
            id = parent;
        }
    }
    refreshNode(node);
    //?????????????????????????????????????????????
    if (underflow) refreshHeader();
    refreshTree(node, id);
    delete node;
    return true;
}

bool IndexHandle::openScan(BufType data, bool lower) {
    int id = _header._root;
    Node *node = getNodeById(id);
    RID rid;
    if (lower) {
        rid.setPageNum(-1);
        rid.setSlotNum(-1);
    } else {
        rid.setPageNum(INT32_MAX);
        rid.setSlotNum(INT32_MAX);
    }
    //?????????????????????????????????????????????
    while (!node->_isLeaf) {
        for (int i = node->_keyNum - 1; i >= 0; i--) {
            //key < data??????data??????
            if (i == 0 || isSmaller(node->_key + i * _header._attrLen, (char *)data, node->_rid[i], rid)) {
                id = node->_child[i];
                node = getNodeById(id);
                break;
            }
        }
    }
    _id = id;
    _pos = -1;
    for (int i = 0; i < node->_keyNum; i++) {
        //key > data
        if (isSmaller((char *)data, node->_key + i * _header._attrLen, rid, node->_rid[i])) {
            _pos = i;
            break;
        }
    }
    //data?????????key?????????????????????
    if (_pos == -1) {
        _id = node->_next;
        _pos = 0;
    }
    delete node;
    //?????????????????????????????????????????????
    return _id != 0;
}

bool IndexHandle::getPrevEntry(RID &rid) {
    //?????????????????????
    if (_id == 0) return false;
    Node *node = getNodeById(_id);
    if (_pos == -1) _pos = node->_keyNum - 1;
    rid = node->_rid[_pos];
    _pos--;
    //????????????????????????????????????????????????????????????
    if (_pos == -1) _id = node->_prev;
    delete node;
    return true;
}

bool IndexHandle::getNextEntry(RID &rid) {
    //?????????????????????
    if (_id == 0) return false;
    Node *node = getNodeById(_id);
    rid = node->_rid[_pos];
    _pos++;
    //???????????????????????????????????????????????????????????????
    if (_pos == node->_keyNum) {
        _id = node->_next;
        _pos = 0;
    }
    delete node;
    return true;
}