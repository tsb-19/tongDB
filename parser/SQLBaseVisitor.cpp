#include <cmath>
#include "SQLBaseVisitor.h"

SQLBaseVisitor::SQLBaseVisitor(SystemManager *systemManager, QueryManager *queryManager) {
    _systemManager = systemManager;
    _queryManager = queryManager;
}

std::any SQLBaseVisitor::visitProgram(SQLParser::ProgramContext *ctx) {
    return visitChildren(ctx);
}

std::any SQLBaseVisitor::visitStatement(SQLParser::StatementContext *ctx) {
    return visitChildren(ctx);
}

std::any SQLBaseVisitor::visitCreate_db(SQLParser::Create_dbContext *ctx) {
    string dbName = _systemManager->getDBName();
    if (!dbName.empty() && !_systemManager->closeDB()) return false;
    if (!_systemManager->createDB(ctx->Identifier()->getText())) return false;
    if (!dbName.empty() && !_systemManager->openDB(dbName)) return false;
    return true;
}

std::any SQLBaseVisitor::visitDrop_db(SQLParser::Drop_dbContext *ctx) {
    string dbName = _systemManager->getDBName();
    if (!dbName.empty()) {
        if (dbName == ctx->Identifier()->getText()) {
            std::cerr << "Can not drop the current database!" << std::endl;
            return false;
        }
        if (!_systemManager->closeDB()) return false;
    }
    if (!_systemManager->dropDB(ctx->Identifier()->getText())) return false;
    if (!dbName.empty() && !_systemManager->openDB(dbName)) return false;
    return true;
}

std::any SQLBaseVisitor::visitShow_dbs(SQLParser::Show_dbsContext *ctx) {
    _systemManager->showDBNames();
    return true;
}

std::any SQLBaseVisitor::visitUse_db(SQLParser::Use_dbContext *ctx) {
    string dbName = _systemManager->getDBName();
    if (!dbName.empty()) if (!_systemManager->closeDB()) return false;
    if (!_systemManager->openDB(ctx->Identifier()->getText())) return false;
    return true;
}

std::any SQLBaseVisitor::visitCreate_table(SQLParser::Create_tableContext *ctx) {
    if (_systemManager->getDBName().empty()) {
        std::cerr << "Please select a database first!" << std::endl;
        return false;
    }
    TableInfo tableInfo;
    tableInfo._tableName = ctx->Identifier()->getText();
    auto result = std::any_cast<std::pair<std::pair<std::any, std::any>, std::pair<std::pair<std::any, std::any>, std::pair<std::any, std::any>>>>(ctx->field_list()->accept(this));
    tableInfo._attrNum = (int) std::any_cast<std::vector<AttrInfo>>(result.first.first).size();
    tableInfo._attrs = std::any_cast<std::vector<AttrInfo>>(result.first.first);
    int recordSize = ceil(tableInfo._attrNum / 8.0);
    std::unordered_set<std::string> attrNames;
    for (const auto &attr : tableInfo._attrs) {
        recordSize += attr._attrLength;
        attrNames.insert(attr._attrName);
    }
    if (attrNames.size() != tableInfo._attrNum) return false;
    tableInfo._recordSize = recordSize;
    auto primaryKeys = std::any_cast<std::vector<std::string>>(result.first.second);
    auto referenceKeys = std::any_cast<std::vector<std::vector<std::string>>>(result.second.first.first);
    auto foreignKeyNames = std::any_cast<std::vector<std::string>>(result.second.first.second);
    auto foreignKeys = std::any_cast<std::vector<std::vector<std::string>>>(result.second.second.first);
    auto references = std::any_cast<std::vector<std::string>>(result.second.second.second);
    tableInfo._indexNum = 0;
    tableInfo._uniqueNum = 0;
    tableInfo._foreignKeyNum = 0;
    if (!_systemManager->createTable(tableInfo)) return false;
    if (!primaryKeys.empty()) {
        if (!_systemManager->createPrimary(tableInfo._tableName, primaryKeys)) return false;
    }
    if (!foreignKeys.empty()) {
        for (int i = 0; i < foreignKeys.size(); i++) {
            if (!_systemManager->createForeign(tableInfo._tableName, foreignKeyNames[i], foreignKeys[i], references[i], referenceKeys[i])) return false;
        }
    }
    return true;
}

std::any SQLBaseVisitor::visitField_list(SQLParser::Field_listContext *ctx) {
    std::vector<AttrInfo> attrs;
    std::vector<std::string> primaryKeys;
    std::vector<std::string> foreignKeyNames;
    std::vector<std::vector<std::string>> foreignKeys;
    std::vector<std::vector<std::string>> referenceKeys;
    std::vector<std::string> references;
    for (const auto &field : ctx->field()) {
        if (dynamic_cast<SQLParser::Normal_fieldContext*>(field) != nullptr) {
            auto attrInfo = std::any_cast<AttrInfo>(field->accept(this));
            attrs.push_back(attrInfo);
        } else if (dynamic_cast<SQLParser::Primary_key_fieldContext*>(field) != nullptr) {
            auto pk = std::any_cast<std::vector<std::string>>(field->accept(this));
            primaryKeys.insert(primaryKeys.begin(), pk.begin(), pk.end());
        } else if (dynamic_cast<SQLParser::Foreign_key_fieldContext*>(field) != nullptr) {
            auto result = std::any_cast<std::pair<std::pair<std::any, std::any>, std::pair<std::any, std::any>>>(field->accept(this));
            foreignKeyNames.push_back(std::any_cast<std::string>(result.second.first));
            foreignKeys.push_back(std::any_cast<std::vector<std::string>>(result.first.first));
            referenceKeys.push_back(std::any_cast<std::vector<std::string>>(result.first.second));
            references.push_back(std::any_cast<std::string>(result.second.second));
        }
    }
    int offset = ceil((double) attrs.size() / 8.0);
    for (auto &attr : attrs) {
        attr._offset = offset;
        offset += attr._attrLength;
    }
    std::pair<std::pair<std::any, std::any>, std::pair<std::pair<std::any, std::any>, std::pair<std::any, std::any>>> result;
    result.first.first = attrs;
    result.first.second = primaryKeys;
    result.second.first.first = referenceKeys;
    result.second.first.second = foreignKeyNames;
    result.second.second.first = foreignKeys;
    result.second.second.second = references;
    return result;
}

std::any SQLBaseVisitor::visitNormal_field(SQLParser::Normal_fieldContext *ctx) {
    AttrInfo attrInfo;
    attrInfo._attrName = ctx->Identifier()->getText();
    attrInfo._attrType = std::any_cast<AttrType>(ctx->type_()->accept(this));
    attrInfo._notNull = ctx->Null() != nullptr;
    attrInfo._isPrimary = false;
    if (attrInfo._attrType != STRING) attrInfo._attrLength = 4;
    else {
        std::string varchar = ctx->type_()->getText();
        std::string length = varchar.substr(8, varchar.length() - 9);
        attrInfo._attrLength = std::stoi(length);
    }
    if (ctx->value() == nullptr) {
        attrInfo._hasDefault = false;
        attrInfo._defaultValue = nullptr;
    } else {
        attrInfo._hasDefault = true;
        if (ctx->value()->Null() != nullptr) attrInfo._defaultValue = nullptr;
        else {
            auto value = ctx->value()->accept(this);
            switch (attrInfo._attrType) {
                case INTEGER:
                    attrInfo._defaultValue = new int;
                    *((int *)attrInfo._defaultValue) = std::any_cast<int>(value);
                    break;
                case FLOAT:
                    attrInfo._defaultValue = new float;
                    *((float *)attrInfo._defaultValue) = std::any_cast<float>(value);
                    break;
                default:
                    attrInfo._defaultValue = new char[attrInfo._attrLength];
                    memset(attrInfo._defaultValue, 0, attrInfo._attrLength);
                    auto str = std::any_cast<std::string>(value);
                    memcpy((char *)attrInfo._defaultValue, str.c_str() + 1, str.length() - 2);
                    break;
            }
        }
    }
    return attrInfo;
}

std::any SQLBaseVisitor::visitPrimary_key_field(SQLParser::Primary_key_fieldContext *ctx) {
    return ctx->identifiers()->accept(this);
}

std::any SQLBaseVisitor::visitForeign_key_field(SQLParser::Foreign_key_fieldContext *ctx) {
    std::string foreignKeyName = ctx->Identifier().size() == 1 ? "foreign_key" : ctx->Identifier()[0]->getText();
    std::string reference = ctx->Identifier().back()->getText();
    auto foreignKey = std::any_cast<std::vector<std::string>>(ctx->identifiers()[0]->accept(this));
    auto referenceKey = std::any_cast<std::vector<std::string>>(ctx->identifiers()[1]->accept(this));
    std::pair<std::pair<std::any, std::any>, std::pair<std::any, std::any>> result;
    result.first.first = foreignKey;
    result.first.second = referenceKey;
    result.second.first = foreignKeyName;
    result.second.second = reference;
    return result;
}

std::any SQLBaseVisitor::visitType_(SQLParser::Type_Context *ctx) {
    if (ctx->getText() == "INT") return INTEGER;
    else if (ctx->getText() == "FLOAT") return FLOAT;
    else return STRING;
}

std::any SQLBaseVisitor::visitValue(SQLParser::ValueContext *ctx) {
    if (ctx->Integer() != nullptr) return std::stoi(ctx->getText());
    else if (ctx->Float() != nullptr) return std::stof(ctx->getText());
    else return ctx->getText();
}

std::any SQLBaseVisitor::visitIdentifiers(SQLParser::IdentifiersContext *ctx) {
    std::vector<std::string> primaryKeys;
    for (const auto &primaryKey : ctx->Identifier()) {
        primaryKeys.push_back(primaryKey->getText());
    }
    return primaryKeys;
}

std::any SQLBaseVisitor::visitDescribe_table(SQLParser::Describe_tableContext *ctx) {
    if (_systemManager->getDBName().empty()) {
        std::cerr << "Please select a database first!" << std::endl;
        return false;
    }
    _systemManager->show(ctx->Identifier()->getText());
    return true;
}

std::any SQLBaseVisitor::visitDrop_table(SQLParser::Drop_tableContext *ctx) {
    if (_systemManager->getDBName().empty()) {
        std::cerr << "Please select a database first!" << std::endl;
        return false;
    }
    return _systemManager->dropTable(ctx->Identifier()->getText());
}

std::any SQLBaseVisitor::visitAlter_table_add_pk(SQLParser::Alter_table_add_pkContext *ctx) {
    if (_systemManager->getDBName().empty()) {
        std::cerr << "Please select a database first!" << std::endl;
        return false;
    }
    auto primaryKeys = std::any_cast<std::vector<std::string>>(ctx->identifiers()->accept(this));
    return _systemManager->createPrimary(ctx->Identifier()[0]->getText(), primaryKeys);
}

std::any SQLBaseVisitor::visitAlter_table_add_foreign_key(SQLParser::Alter_table_add_foreign_keyContext *ctx) {
    if (_systemManager->getDBName().empty()) {
        std::cerr << "Please select a database first!" << std::endl;
        return false;
    }
    std::string tableName = ctx->Identifier().front()->getText();
    std::string foreignKeyName = ctx->Identifier().size() == 2 ? "foreign_key" : ctx->Identifier()[1]->getText();
    auto foreignKeys = std::any_cast<std::vector<std::string>>(ctx->identifiers()[0]->accept(this));
    auto referenceKeys = std::any_cast<std::vector<std::string>>(ctx->identifiers()[1]->accept(this));
    std::string reference = ctx->Identifier().back()->getText();
    return _systemManager->createForeign(tableName, foreignKeyName, foreignKeys, reference, referenceKeys);
}

std::any SQLBaseVisitor::visitAlter_table_add_unique(SQLParser::Alter_table_add_uniqueContext *ctx) {
    if (_systemManager->getDBName().empty()) {
        std::cerr << "Please select a database first!" << std::endl;
        return false;
    }
    auto uniqueKeys = std::any_cast<std::vector<std::string>>(ctx->identifiers()->accept(this));
    return _systemManager->createUnique(ctx->Identifier()->getText(), uniqueKeys);
}

std::any SQLBaseVisitor::visitAlter_table_drop_pk(SQLParser::Alter_table_drop_pkContext *ctx) {
    if (_systemManager->getDBName().empty()) {
        std::cerr << "Please select a database first!" << std::endl;
        return false;
    }
    return _systemManager->dropPrimary(ctx->Identifier()[0]->getText());
}

std::any SQLBaseVisitor::visitAlter_table_drop_foreign_key(SQLParser::Alter_table_drop_foreign_keyContext *ctx) {
    if (_systemManager->getDBName().empty()) {
        std::cerr << "Please select a database first!" << std::endl;
        return false;
    }
    return _systemManager->dropForeign(ctx->Identifier()[0]->getText(), ctx->Identifier()[1]->getText());
}

std::any SQLBaseVisitor::visitAlter_add_index(SQLParser::Alter_add_indexContext *ctx) {
    if (_systemManager->getDBName().empty()) {
        std::cerr << "Please select a database first!" << std::endl;
        return false;
    }
    auto indexes = std::any_cast<std::vector<std::string>>(ctx->identifiers()->accept(this));
    return _systemManager->createIndex(ctx->Identifier()->getText(), indexes);
}

std::any SQLBaseVisitor::visitAlter_drop_index(SQLParser::Alter_drop_indexContext *ctx) {
    if (_systemManager->getDBName().empty()) {
        std::cerr << "Please select a database first!" << std::endl;
        return false;
    }
    auto indexes = std::any_cast<std::vector<std::string>>(ctx->identifiers()->accept(this));
    return _systemManager->dropIndex(ctx->Identifier()->getText(), indexes);
}

std::any SQLBaseVisitor::visitShow_tables(SQLParser::Show_tablesContext *ctx) {
    if (_systemManager->getDBName().empty()) {
        std::cerr << "Please select a database first!" << std::endl;
        return false;
    }
    _systemManager->showTableNames();
    return true;
}

std::any SQLBaseVisitor::visitShow_indexes(SQLParser::Show_indexesContext *ctx) {
    if (_systemManager->getDBName().empty()) {
        std::cerr << "Please select a database first!" << std::endl;
        return false;
    }
    _systemManager->showIndexNames();
    return true;
}

std::any SQLBaseVisitor::visitValue_lists(SQLParser::Value_listsContext *ctx) {
    std::vector<std::vector<Value>> value_lists;
    for (const auto &value_list : ctx->value_list()) {
        value_lists.push_back(std::any_cast<std::vector<Value>>(value_list->accept(this)));
    }
    return value_lists;
}

std::any SQLBaseVisitor::visitValue_list(SQLParser::Value_listContext *ctx) {
    std::vector<Value> value_list;
    for (const auto &value_context : ctx->value()) {
        Value value{STRING, nullptr};
        if (value_context->Integer() != nullptr) {
            value._attrType = INTEGER;
            value._data = new int;
            *((int *)value._data) = std::any_cast<int>(value_context->accept(this));
        } else if (value_context->Float() != nullptr) {
            value._attrType = FLOAT;
            value._data = new float;
            *((float *)value._data) = std::any_cast<float>(value_context->accept(this));
        } else if (value_context->String() != nullptr) {
            value._attrType = STRING;
            auto str = std::any_cast<std::string>(value_context->accept(this));
            value._data = new char[str.length() - 1];
            memset(value._data, 0, str.length() - 1);
            memcpy((char *)value._data, str.c_str() + 1, str.length() - 2);
        }
        value_list.push_back(value);
    }
    return value_list;
}

std::any SQLBaseVisitor::visitInsert_into_table(SQLParser::Insert_into_tableContext *ctx) {
    if (_systemManager->getDBName().empty()) {
        std::cerr << "Please select a database first!" << std::endl;
        return false;
    }
    auto value_list = std::any_cast<std::vector<std::vector<Value>>>(ctx->value_lists()->accept(this));
    bool ok = _queryManager->insertData(ctx->Identifier()->getText(), value_list);
    for (const auto &values : value_list) {
        for (const auto &value : values) {
            if (value._data != nullptr) {
                if (value._attrType == INTEGER) delete (int *) value._data;
                else if (value._attrType == FLOAT) delete (float *) value._data;
                else delete[](char *) value._data;
            }
        }
    }
    return ok;
}

std::any SQLBaseVisitor::visitDelete_from_table(SQLParser::Delete_from_tableContext *ctx) {
    if (_systemManager->getDBName().empty()) {
        std::cerr << "Please select a database first!" << std::endl;
        return false;
    }
    std::string tableName = ctx->Identifier()->getText();
    auto conditions = std::any_cast<std::vector<Condition>>(ctx->where_and_clause()->accept(this));
    for (auto &condition : conditions) {
        condition._lhsAttr._relName = tableName;
        if (condition._rhsIsAttr) condition._rhsAttr._relName = tableName;
    }
    bool ok = _queryManager->deleteData(tableName, conditions);
    for (const auto &condition : conditions) {
        if (!condition._rhsIsAttr && condition._rhsValue._data != nullptr) {
            if (condition._rhsValue._attrType == INTEGER) delete (int *) condition._rhsValue._data;
            else if (condition._rhsValue._attrType == FLOAT) delete (float *) condition._rhsValue._data;
            else delete[](char *) condition._rhsValue._data;
        }
    }
    return ok;
}

std::any SQLBaseVisitor::visitUpdate_table(SQLParser::Update_tableContext *ctx) {
    std::string tableName = ctx->Identifier()->getText();
    auto result = std::any_cast<std::pair<std::vector<RelAttr>, std::vector<Value>>>(ctx->set_clause()->accept(this));
    for (auto &relAttr : result.first) {
        relAttr._relName = tableName;
    }
    auto conditions = std::any_cast<std::vector<Condition>>(ctx->where_and_clause()->accept(this));
    for (auto &condition : conditions) {
        condition._lhsAttr._relName = tableName;
        if (condition._rhsIsAttr) condition._rhsAttr._relName = tableName;
    }
    bool ok = _queryManager->updateData(tableName, result.first, result.second, conditions);
    for (const auto &value : result.second) {
        if (value._data != nullptr) {
            if (value._attrType == INTEGER) delete (int *) value._data;
            else if (value._attrType == FLOAT) delete (float *) value._data;
            else delete[](char *) value._data;
        }
    }
    for (const auto &condition : conditions) {
        if (!condition._rhsIsAttr && condition._rhsValue._data != nullptr) {
            if (condition._rhsValue._attrType == INTEGER) delete (int *) condition._rhsValue._data;
            else if (condition._rhsValue._attrType == FLOAT) delete (float *) condition._rhsValue._data;
            else delete[](char *) condition._rhsValue._data;
        }
    }
    return ok;
}

std::any SQLBaseVisitor::visitSelect_table_(SQLParser::Select_table_Context *ctx) {
    if (_systemManager->getDBName().empty()) {
        std::cerr << "Please select a database first!" << std::endl;
        return false;
    }
    return std::any_cast<bool>(ctx->select_table()->accept(this));
}

std::any SQLBaseVisitor::visitSelect_table(SQLParser::Select_tableContext *ctx) {
    auto tableNames = std::any_cast<std::vector<std::string>>(ctx->identifiers()->accept(this));
    auto relAttrs = std::any_cast<std::vector<RelAttr>>(ctx->selectors()->accept(this));
    for (auto &relAttr : relAttrs) {
        if (tableNames.size() == 1) relAttr._relName = tableNames[0];
        else if (relAttr._relName.empty()) {
            std::cerr << "Column " + relAttr._attrName + " is ambiguous!" << std::endl;
            return false;
        }
    }
    std::vector<Condition> conditions;
    if (ctx->where_and_clause() != nullptr) {
        conditions = std::any_cast<std::vector<Condition>>(ctx->where_and_clause()->accept(this));
        for (auto &condition : conditions) {
            if (tableNames.size() == 1) condition._lhsAttr._relName = tableNames[0];
            else if (condition._lhsAttr._relName.empty()) {
                std::cerr << "Column " + condition._lhsAttr._relName + " is ambiguous!" << std::endl;
                return false;
            }
            if (condition._rhsIsAttr) {
                if (tableNames.size() == 1) condition._rhsAttr._relName = tableNames[0];
                else if (condition._rhsAttr._relName.empty()) {
                    std::cerr << "Column " + condition._rhsAttr._relName + " is ambiguous!" << std::endl;
                    return false;
                }
            }
        }
    }
    int limit = INT32_MAX;
    int offset = 0;
    if (ctx->getText().find("LIMIT") != ctx->getText().npos) limit = std::stoi(ctx->Integer().front()->getText());
    if (ctx->getText().find("OFFSET") != ctx->getText().npos) offset = std::stoi(ctx->Integer().back()->getText());
    bool ok = _queryManager->selectData(tableNames, relAttrs, conditions, limit, offset);
    if (ctx->where_and_clause() != nullptr) {
        for (const auto &condition : conditions) {
            if (!condition._rhsIsAttr) {
                if (condition._rhsValue._data != nullptr) {
                    if (condition._rhsValue._attrType == INTEGER) delete (int *) condition._rhsValue._data;
                    else if (condition._rhsValue._attrType == FLOAT) delete (float *) condition._rhsValue._data;
                    else delete[](char *) condition._rhsValue._data;
                } else {
                    for (const auto &value : condition._rhsValues) {
                        if (value._attrType == INTEGER) delete (int *) value._data;
                        else if (value._attrType == FLOAT) delete (float *) value._data;
                        else delete[](char *) value._data;
                    }
                }
            }
        }
    }
    return ok;
}

std::any SQLBaseVisitor::visitSet_clause(SQLParser::Set_clauseContext *ctx) {
    std::pair<std::vector<RelAttr>, std::vector<Value>> result;
    for (const auto &identifier : ctx->Identifier()) {
        RelAttr relAttr;
        relAttr._attrName = identifier->getText();
        result.first.push_back(relAttr);
    }
    for (const auto &value_context : ctx->value()) {
        Value value{STRING, nullptr};
        if (value_context->Integer() != nullptr) {
            value._attrType = INTEGER;
            value._data = new int;
            *((int *)value._data) = std::any_cast<int>(value_context->accept(this));
        } else if (value_context->Float() != nullptr) {
            value._attrType = FLOAT;
            value._data = new float;
            *((float *)value._data) = std::any_cast<float>(value_context->accept(this));
        } else if (value_context->String() != nullptr) {
            value._attrType = STRING;
            auto str = std::any_cast<std::string>(value_context->accept(this));
            value._data = new char[str.length() - 1];
            memset(value._data, 0, str.length() - 1);
            memcpy((char *)value._data, str.c_str() + 1, str.length() - 2);
        }
        result.second.push_back(value);
    }
    return result;
}

std::any SQLBaseVisitor::visitWhere_and_clause(SQLParser::Where_and_clauseContext *ctx) {
    std::vector<Condition> conditions;
    for (const auto &clause : ctx->where_clause()) {
        conditions.push_back(std::any_cast<Condition>(clause->accept(this)));
    }
    return conditions;
}

std::any SQLBaseVisitor::visitWhere_operator_expression(SQLParser::Where_operator_expressionContext *ctx) {
    Condition condition;
    condition._lhsAttr = std::any_cast<RelAttr>(ctx->column()->accept(this));
    condition._op = std::any_cast<CompOp>(ctx->operator_()->accept(this));
    condition._rhsIsAttr = (ctx->expression()->value() == nullptr);
    auto expression = ctx->expression()->accept(this);
    if (ctx->expression()->value() != nullptr) condition._rhsValue = std::any_cast<Value>(expression);
    else condition._rhsAttr = std::any_cast<RelAttr>(expression);
    return condition;
}

std::any SQLBaseVisitor::visitWhere_in_list(SQLParser::Where_in_listContext *ctx) {
    Condition condition;
    condition._lhsAttr = std::any_cast<RelAttr>(ctx->column()->accept(this));
    condition._op = NO_OP;
    condition._rhsIsAttr = false;
    condition._rhsValue._data = nullptr;
    condition._rhsValues = std::any_cast<std::vector<Value>>(ctx->value_list()->accept(this));
    return condition;
}

std::any SQLBaseVisitor::visitWhere_null(SQLParser::Where_nullContext *ctx) {
    Condition condition;
    condition._lhsAttr = std::any_cast<RelAttr>(ctx->column()->accept(this));
    condition._op = ctx->children.size() == 3 ? IS_NULL : IS_NOT_NULL;
    condition._rhsIsAttr = false;
    condition._rhsValue._data = nullptr;
    return condition;
}

std::any SQLBaseVisitor::visitColumn(SQLParser::ColumnContext *ctx) {
    RelAttr relAttr;
    if (ctx->Identifier().size() == 2) relAttr._relName = ctx->Identifier()[0]->getText();
    relAttr._attrName = ctx->Identifier().back()->getText();
    return relAttr;
}

std::any SQLBaseVisitor::visitExpression(SQLParser::ExpressionContext *ctx) {
    if (ctx->value() != nullptr) {
        Value value{STRING, nullptr};
        if (ctx->value()->Integer() != nullptr) {
            value._attrType = INTEGER;
            value._data = new int;
            *((int *)value._data) = std::any_cast<int>(ctx->value()->accept(this));
        } else if (ctx->value()->Float() != nullptr) {
            value._attrType = FLOAT;
            value._data = new float;
            *((float *)value._data) = std::any_cast<float>(ctx->value()->accept(this));
        } else if (ctx->value()->String() != nullptr) {
            value._attrType = STRING;
            auto str = std::any_cast<std::string>(ctx->value()->accept(this));
            value._data = new char[str.length() - 1];
            memset(value._data, 0, str.length() - 1);
            memcpy((char *)value._data, str.c_str() + 1, str.length() - 2);
        } else {
            std::cerr << "Filter value cannot be null!" << std::endl;
        }
        return value;
    } else {
        return std::any_cast<RelAttr>(ctx->column()->accept(this));
    }
}

std::any SQLBaseVisitor::visitSelectors(SQLParser::SelectorsContext *ctx) {
    std::vector<RelAttr> relAttrs;
    for (const auto &selector : ctx->selector()) {
        relAttrs.push_back(std::any_cast<RelAttr>(selector->accept(this)));
    }
    return relAttrs;
}

std::any SQLBaseVisitor::visitSelector(SQLParser::SelectorContext *ctx) {
    RelAttr relAttr;
    if (ctx->column() != nullptr) relAttr = std::any_cast<RelAttr>(ctx->column()->accept(this));
    return relAttr;
}

std::any SQLBaseVisitor::visitOperator_(SQLParser::Operator_Context *ctx) {
    if (ctx->EqualOrAssign() != nullptr) return EQ_OP;
    else if (ctx->Less() != nullptr) return LT_OP;
    else if (ctx->LessEqual() != nullptr) return LE_OP;
    else if (ctx->Greater() != nullptr) return GT_OP;
    else if (ctx->GreaterEqual() != nullptr) return GE_OP;
    else if (ctx->NotEqual() != nullptr) return NE_OP;
    else return NO_OP;
}

std::any SQLBaseVisitor::visitLoad_data(SQLParser::Load_dataContext *ctx) {
    if (_systemManager->getDBName().empty()) {
        std::cerr << "Please select a database first!" << std::endl;
        return false;
    }
    std::string tableName = ctx->Identifier()->getText();
    int table_id = _systemManager->getTableIDByName(tableName);
    if (table_id == -1) {
        cerr << "Table " << tableName << " does not exist!" << endl;
        return false;
    }
    const TableInfo &tableInfo = _systemManager->getTableInfoByID(table_id);
    std::string fileName = ctx->String()->getText().substr(1, ctx->String()->getText().length() - 2);
    std::ifstream fin(fileName);
    if (fin.fail()) {
        std::cerr << "Open file " + fileName + " failed!" << std::endl;
        return false;
    }
    std::string line;
    std::vector<std::vector<Value>> value_list;
    while (std::getline(fin, line)) {
        std::stringstream ss(line);
        std::string str;
        std::vector<Value> values;
        for (const auto &attr : tableInfo._attrs) {
            std::getline(ss, str, ',');
            Value value{attr._attrType, nullptr};
            if (str != "NULL") {
                switch (attr._attrType) {
                    case INTEGER:
                        value._data = new int;
                        *((int *) value._data) = stoi(str);
                        break;
                    case FLOAT:
                        value._data = new float;
                        *((float *) value._data) = stof(str);
                        break;
                    default:
                        value._data = new char[str.length() + 1];
                        memset(value._data, 0, str.length() + 1);
                        memcpy((char *) value._data, str.c_str(), str.length());
                        break;
                }
            }
            values.push_back(value);
        }
        value_list.push_back(values);
    }
    bool ok = _queryManager->insertData(tableName, value_list);
    for (const auto &values : value_list) {
        for (const auto &value: values) {
            if (value._data != nullptr) {
                if (value._attrType == INTEGER) delete (int *) value._data;
                else if (value._attrType == FLOAT) delete (float *) value._data;
                else delete[](char *) value._data;
            }
        }
    }
    fin.close();
    return ok;
}

std::any SQLBaseVisitor::visitDump_data(SQLParser::Dump_dataContext *ctx) {
    if (_systemManager->getDBName().empty()) {
        std::cerr << "Please select a database first!" << std::endl;
        return false;
    }
    std::string tableName = ctx->Identifier()->getText();
    int table_id = _systemManager->getTableIDByName(tableName);
    if (table_id == -1) {
        cerr << "Table " << tableName << " does not exist!" << endl;
        return false;
    }
    const TableInfo &tableInfo = _systemManager->getTableInfoByID(table_id);
    std::string fileName = ctx->String()->getText().substr(1, ctx->String()->getText().length() - 2);
    std::ofstream fout(fileName);
    if (fout.fail()) {
        std::cerr << "Open file " + fileName + " failed!" << std::endl;
        return false;
    }
    auto backup = std::cout.rdbuf();
    std::cout.rdbuf(fout.rdbuf());
    std::vector<RelAttr> relAttrs;
    std::vector<Condition> conditions;
    _queryManager->filterTable(tableInfo, conditions, [tableInfo, relAttrs](const RID &rid, const char *data) -> bool {
        for (int i = 0; i < tableInfo._attrs.size(); i++) {
            if ((data[i >> 3] >> (i & 7)) & 1) std::cout << "NULL";
            else if (tableInfo._attrs[i]._attrType == INTEGER) {
                int a;
                memcpy(&a, data + tableInfo._attrs[i]._offset, tableInfo._attrs[i]._attrLength);
                std::cout << a;
            } else if (tableInfo._attrs[i]._attrType == FLOAT) {
                float a;
                memcpy(&a, data + tableInfo._attrs[i]._offset, tableInfo._attrs[i]._attrLength);
                std::cout << a;
            } else {
                char *a = new char[tableInfo._attrs[i]._attrLength];
                memcpy(a, data + tableInfo._attrs[i]._offset, tableInfo._attrs[i]._attrLength);
                std::cout << a;
                delete[] a;
            }
            std::cout << (i != tableInfo._attrs.size() - 1 ? ',' : '\n');
        }
        return true;
    });
    std::cout.rdbuf(backup);
    fout.close();
    return true;
}