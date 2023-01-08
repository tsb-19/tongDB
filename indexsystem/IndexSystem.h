#ifndef INDEX_SYSTEM_H
#define INDEX_SYSTEM_H

#include <vector>
#include "../recordsystem/RecordSystem.h"

struct IndexHeader {
    int _attrNum;//索引字段个数
    int _attrLen;//索引字段总大小，单位：字节
    int _root;//根节点id
    int _maxChildNum;//节点最大数量
    int _firstEmptyPage;//第一个空闲页面，等于0说明没有空闲页面，要分配新的页面
    int _pageNumber;//目前分配的页面总数
    int _keyStart, _childStart, _ridStart;//偏移量
    //IndexHeader的后边是每个字段的类型和长度，可以根据_attrNum计算偏移得到
};

struct Node {
    BufType _start;//节点存储首地址
    int _index;//节点存储数组下标

    int _isLeaf;//是否为叶节点
    int _keyNum;//键数量
    int _parent;//父节点id
    int _prev, _next;//叶结点的前驱、后继
    int _nextEmptyPage;//下一个空闲页面，等于0说明没有空闲页面

    char *_key;//键，即索引
    int *_child;//子节点id
    RID *_rid;//索引对应的页号、槽号
};

class IndexHandle {
private:
    BufPageManager *_bufPageManager;//缓存页面管理
    int _fileID;//管理的文件标识符
    struct IndexHeader _header;//第一个页面记录信息头
    std::vector<AttrType> _attrTypes;//每个索引字段的类型
    std::vector<int> _attrLens;//每个索引字段的长度
    int _id;//当前扫描到的节点
    int _pos;//当前扫描到的节点内部位置
    bool isSmaller(const char *data1, const char *data2, const RID &rid1, const RID &rid2);//比较索引大小
    Node *getNodeById(int id, bool isNew = false) const;//根据id获得对应节点
    void refreshNode(Node *node) const;//标记节点被修改
    void refreshHeader() const;//标记信息头被修改
    void refreshTree(Node *&node, int id) const;//更新祖先节点
public:
    IndexHandle(BufPageManager *bufPageManager, int fileID);
    ~IndexHandle() {};
    bool insertEntry(BufType data, const RID &rid, bool isUnique, bool check);//根据data和rid插入一条索引，若isUnique为true且data重复返回false，若check为true仅用作检查
    bool deleteEntry(BufType data, const RID &rid);//根据data和rid删除对应索引
    bool openScan(BufType data, bool lower);//从data开始扫描，_id和_pod设为第一条索引位置，lower参数与stl查找相同
    bool getPrevEntry(RID &rid);//rid返回索引指向的记录位置，访问完所有索引返回false
    bool getNextEntry(RID &rid);//rid返回索引指向的记录位置，访问完所有索引返回false
};

class IndexManager {
private:
    BufPageManager *_bufPageManager;
    FileManager *_fileManager;
public:
    IndexManager(BufPageManager *bufPageManager, FileManager *fileManager);
    ~IndexManager() {};
    //根据文件名和索引名称创建索引文件，attrLen为索引字段总大小，单位：字节
    bool createIndex(const char *fileName, const std::vector<std::string> &attrNames, int attrNum, const int *attrLens, const AttrType *attrTypes);
    bool destroyIndex(const char *fileName, const std::vector<std::string> &attrNames);//根据文件名和索引名称删除相应索引文件
    bool openIndex(const char *fileName, const std::vector<std::string> &attrNames, int &fileID);//打开索引文件，fileID返回文件标识符
    bool closeIndex(int fileID);//根据指定的标识符关闭相应的索引文件
};

#endif //INDEX_SYSTEM_H
