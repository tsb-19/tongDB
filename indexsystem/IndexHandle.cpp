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
    //先检查叶结点中是否有重复主键
    if (isUnique) {
        Node *node = getNodeById(id);
        RID r(-1, -1);
        //从根节点开始搜索到比较位置
        while (!node->_isLeaf) {
            for (int i = node->_keyNum - 1; i >= 0; i--) {
                //key < data，或data最小
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
        //重复主键可能在当前节点或其后继
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
    //从根节点开始搜索到插入位置
    while (!node->_isLeaf) {
        for (int i = node->_keyNum - 1; i >= 0; i--) {
            //key < data，或data最小
            if (i == 0 || isSmaller(node->_key + i * _header._attrLen, (char *)data, node->_rid[i], rid)) {
                id = node->_child[i];
                node = getNodeById(id);
                break;
            }
        }
    }
    int pos = node->_keyNum - 1;
    while (pos >= 0) {
        //第一次出现key < data即为插入位置
        if (isSmaller(node->_key + pos * _header._attrLen, (char *)data, node->_rid[pos], rid)) {
            memcpy(node->_key + (pos + 1) * _header._attrLen, data, _header._attrLen);
            node->_rid[pos + 1] = rid;
            break;
        }
        //将数据右移空出位置
        memcpy(node->_key + (pos + 1) * _header._attrLen, node->_key + pos * _header._attrLen, _header._attrLen);
        node->_rid[pos + 1] = node->_rid[pos];
        pos--;
    }
    //说明data是当前叶节点的最小值
    if (pos == -1) {
        memcpy(node->_key, data, _header._attrLen);
        node->_rid[0] = rid;
    }
    node->_keyNum++;
    bool overflow = false;
    //发生上溢
    if (node->_keyNum == _header._maxChildNum) {
        overflow = true;
        //递归处理上溢情况
        while (node->_keyNum == _header._maxChildNum) {
            int parent = node->_parent;
            Node *parentNode;
            if (parent != 0) parentNode = getNodeById(parent);
            //发生上溢的是根节点
            else {
                //有空闲页直接使用，否则分配新的页面
                if (_header._firstEmptyPage != 0) {
                    parentNode = getNodeById(_header._firstEmptyPage, true);
                    parent = _header._firstEmptyPage;
                    _header._firstEmptyPage = parentNode->_nextEmptyPage;
                } else {
                    _header._pageNumber++;
                    parent = _header._pageNumber;
                    parentNode = getNodeById(_header._pageNumber, true);
                }
                //初始化新根节点
                parentNode->_isLeaf = false;
                parentNode->_keyNum = 1;
                parentNode->_parent = 0;
                parentNode->_nextEmptyPage = 0;
                memcpy(parentNode->_key, node->_key, _header._attrLen);
                parentNode->_child[0] = id;
                parentNode->_rid[0] = node->_rid[0];
                node->_parent = parent;
                //更新根节点id
                _header._root = parent;
            }
            //找到子节点在父节点中的位置
            int i = 0;
            while (parentNode->_child[i] != id) i++;
            //将父节点数据右移空出位置
            for (int j = parentNode->_keyNum - 1; j > i; j--) {
                memcpy(parentNode->_key + (j + 1) * _header._attrLen, parentNode->_key + j * _header._attrLen, _header._attrLen);
                parentNode->_child[j + 1] = parentNode->_child[j];
                parentNode->_rid[j + 1] = parentNode->_rid[j];
            }
            parentNode->_keyNum++;
            //分裂新节点
            Node *newNode;
            int newID;
            //和分裂根节点基本相同
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
            //新节点的属性和原节点相同
            newNode->_isLeaf = node->_isLeaf;
            newNode->_keyNum = node->_keyNum - node->_keyNum / 2;
            newNode->_parent = node->_parent;
            newNode->_nextEmptyPage = 0;
            node->_keyNum /= 2;
            //将原节点的后一半数据移动到新节点
            for (int j = 0; j < newNode->_keyNum; j++) {
                memcpy(newNode->_key + j * _header._attrLen, node->_key + (node->_keyNum + j) * _header._attrLen, _header._attrLen);
                newNode->_child[j] = node->_child[node->_keyNum + j];
                //注意要修改对应子节点的父节点id
                if (!newNode->_isLeaf && newNode->_child[j] != 0) {
                    Node *childNode = getNodeById(newNode->_child[j]);
                    childNode->_parent = newID;
                    refreshNode(childNode);
                    delete childNode;
                }
                newNode->_rid[j] = node->_rid[node->_keyNum + j];
            }
            //如果新节点是叶节点，要设置其前驱和后继
            if (newNode->_isLeaf) {
                newNode->_prev = id;
                newNode->_next = node->_next;
                //找到原节点的后继，修改其前驱
                if (node->_next != 0) {
                    Node *brotherNode = getNodeById(node->_next);
                    brotherNode->_prev = newID;
                    refreshNode(brotherNode);
                    delete brotherNode;
                }
                node->_next = newID;
            }
            //修改父节点的数据
            memcpy(parentNode->_key + i * _header._attrLen, node->_key, _header._attrLen);
            memcpy(parentNode->_key + (i + 1) * _header._attrLen, newNode->_key, _header._attrLen);
            parentNode->_rid[i] = node->_rid[0];
            parentNode->_rid[i + 1] = newNode->_rid[0];
            refreshNode(node);
            refreshNode(newNode);
            delete node;
            delete newNode;
            //递归处理可能新发生的上溢
            id = parent;
            node = parentNode;
        }
    }
    refreshNode(node);
    //如果发生了上溢，需要修改信息头
    if (overflow) refreshHeader();
    refreshTree(node, id);
    delete node;
    return true;
}

bool IndexHandle::deleteEntry(BufType data, const RID &rid) {
    int id = _header._root;
    Node *node = getNodeById(id);
    //从根节点开始搜索到删除位置
    while (!node->_isLeaf) {
        for (int i = node->_keyNum - 1; i >= 0; i--) {
            //key < data，或data最小
            if (i == 0 || !isSmaller((char *)data, node->_key + i * _header._attrLen, rid, node->_rid[i])) {
                id = node->_child[i];
                node = getNodeById(id);
                break;
            }
        }
    }
    int pos = node->_keyNum - 1;
    while (pos >= 0) {
        //找到删除位置
        if (!isSmaller((char *)data, node->_key + pos * _header._attrLen, rid, node->_rid[pos])) {
            break;
        }
        pos--;
    }
    //没有找到要删除的索引，返回false
    if (pos == -1) {
        delete node;
        return false;
    }
    node->_keyNum--;
    //将数据左移删除数据
    while (pos < node->_keyNum) {
        memcpy(node->_key + pos * _header._attrLen, node->_key + (pos + 1) * _header._attrLen, _header._attrLen);
        node->_rid[pos] = node->_rid[pos + 1];
        pos++;
    }
    bool underflow = false;
    //发生下溢
    if (node->_keyNum == 0) {
        underflow = true;
        //修改前驱和后继
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
        //递归处理下溢情况
        while (node->_keyNum == 0) {
            //到根节点终止
            if (id == _header._root) {
                if (node->_keyNum == 0) node->_isLeaf = true;
                break;
            }
            int parent = node->_parent;
            Node *parentNode = getNodeById(parent);
            //找到子节点在父节点中的位置
            int i = 0;
            while (parentNode->_child[i] != id) i++;
            parentNode->_keyNum--;
            //将数据左移删除该子节点
            while (i < parentNode->_keyNum) {
                memcpy(parentNode->_key + i * _header._attrLen, parentNode->_key + (i + 1) * _header._attrLen, _header._attrLen);
                parentNode->_child[i] = parentNode->_child[i + 1];
                parentNode->_rid[i] = parentNode->_rid[i + 1];
                i++;
            }
            //回收页面
            node->_nextEmptyPage = _header._firstEmptyPage;
            _header._firstEmptyPage = id;
            refreshNode(node);
            delete node;
            node = parentNode;
            //递归处理可能新发生的下溢
            id = parent;
        }
    }
    refreshNode(node);
    //如果发生了下溢，需要修改信息头
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
    //从根节点开始搜索到开始扫描位置
    while (!node->_isLeaf) {
        for (int i = node->_keyNum - 1; i >= 0; i--) {
            //key < data，或data最小
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
    //data比所有key大，从后继开始
    if (_pos == -1) {
        _id = node->_next;
        _pos = 0;
    }
    delete node;
    //没有后继说明没有符合要求的记录
    return _id != 0;
}

bool IndexHandle::getPrevEntry(RID &rid) {
    //访问完所有索引
    if (_id == 0) return false;
    Node *node = getNodeById(_id);
    if (_pos == -1) _pos = node->_keyNum - 1;
    rid = node->_rid[_pos];
    _pos--;
    //已经访问完当前节点第一个值，访问前驱节点
    if (_pos == -1) _id = node->_prev;
    delete node;
    return true;
}

bool IndexHandle::getNextEntry(RID &rid) {
    //访问完所有索引
    if (_id == 0) return false;
    Node *node = getNodeById(_id);
    rid = node->_rid[_pos];
    _pos++;
    //已经访问完当前节点最后一个值，访问后继节点
    if (_pos == node->_keyNum) {
        _id = node->_next;
        _pos = 0;
    }
    delete node;
    return true;
}