#ifndef RECORD_MANAGE_H
#define RECORD_MANAGE_H

#include "../filesystem/FileSystem.h"

enum AttrType {
    INTEGER,
    FLOAT,
    STRING
};

class RID {
private:
    int _pageNum;//页号
    int _slotNum;//槽号
public:
    RID() {};
    RID(int pageNum, int slotNum) : _pageNum(pageNum), _slotNum(slotNum) {};
    ~RID() {};
    void setPageNum(int pageNum) { _pageNum = pageNum; }
    void setSlotNum(int slotNum) { _slotNum = slotNum; }
    const int &getPageNum() const { return _pageNum; }
    const int &getSlotNum() const { return _slotNum; }

    bool operator==(const RID &rhs) const {
        return (this->_pageNum == rhs._pageNum && this->_slotNum == rhs._slotNum);
    }
};

struct RecordHeader {
    int _recordSize;//一条记录所占空间，单位：字节
    int _recordCount;//一个页面可以容纳的记录条数
    int _bitmapSize;//一个页面的位图所占空间，单位：字节
    int _firstEmptyPage;//第一个空闲页面，等于0说明没有空闲页面，要分配新的页面
    int _pageNumber;//目前分配的页面总数
};

const int nextPageOffset = sizeof(int);//每个页面用四个字节记录下一个空闲页面

class RecordHandle {
private:
    BufPageManager *_bufPageManager;//缓存页面管理
    int _fileID;//管理的文件标识符
    struct RecordHeader _header;//第一个页面记录信息头
    RID _rid;//当前扫描到的位置
    int getFirstZeroBit(BufType b, int size) const;//获得第一个空闲槽位置
    void refreshHeader() const;//标记信息头被修改
public:
    RecordHandle(BufPageManager *bufPageManager, int fileID);
    ~RecordHandle() {};
    void getRecord(const RID &rid, BufType data);//根据rid获得记录，将数据传入data中
    bool insertRecord(BufType data, RID &rid);//将data插入第一个空闲槽，rid返回记录位置
    bool deleteRecord(const RID &rid);//根据rid删除记录
    bool updateRecord(const RID &rid, BufType data);//将位置为rid的记录数据更新为data
    bool openScan();//开始扫描，将_rid设置为第一条记录的位置
    bool getNextRecord(RID &rid, BufType data);//data返回当前扫描的数据，rid返回数据位置，访问完所有记录返回false
};

class RecordManager {
private:
    BufPageManager *_bufPageManager;
    FileManager *_fileManager;
public:
    RecordManager(BufPageManager *bufPageManager, FileManager *fileManager);
    ~RecordManager() {};
    bool createFile(const char *fileName, int recordSize);//根据文件名创建文件，recordSize为一条记录的大小，单位：字节
    bool destroyFile(const char *fileName);//根据文件名删除相应文件
    bool openFile(const char *fileName, int &fileID);//打开文件，fileID返回文件标识符
    bool closeFile(int fileID);//根据指定的标识符关闭相应的文件
};

#endif //RECORD_MANAGE_H
