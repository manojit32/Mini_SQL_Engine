import re,sys,csv,sqlparse,copy
COND_VALUES=("AND", "OR", "(", ")")
OP_VALUES=("=", ">", "<", "!=", "<=", ">=")
import collections
dataset=collections.defaultdict(list)
def main():
    global database_meta, query, database, database1, distinct_flag,schema
    database_meta = {}
    database = {}
    database1={}
    schema={}
    arguments = sys.argv[1:]
    count_arguments = len(arguments)
    if count_arguments == 1:
        ProcessMetaData()
        ProcessData()
        query = (sys.argv[1])
        ProcessQuery()
    else:
        print("invalid syntax\n usage: python mini_sql.py <sql_query>")

def check_col_table(column_name,tables):
    if "." in column_name:
        return column_name
    found=[]
    for table in tables:
        temp=table+"."+column_name
        if temp in schema[table]:
            found.append(table)
    if len(found)==0:
        print("Could not find the column in table")
        return
    elif len(found)>1:
        print("Presence of conflicting columns")
        return
    return found[0]+"."+column_name



def remove_quote(s):     
    s = s.strip()
    while len(s) > 1 and (s[0]=='\"' or s[0]=='\'') and s[0]==s[-1]:
        s = s[1:-1] 
    return s

def ProcessQuery():
    #try:
    if query[len(query)-1]!=';':
        print("Invalid Query Syntax")
        exit(0)
    else:
        query1=query.replace(';','')
    formatted_query=sqlparse.format(query1,reindent=True,keyword_case='upper')
    processed_sql_query = sqlparse.split(query1)
    temp = []
    for meta in processed_sql_query:
        temp.append(sqlparse.format(meta,keyword_case="upper",identifier_case="lower",strip_comments=True))
    processed_sql_query = temp

    for sql_stmt in processed_sql_query:
        parsed_stmt = sqlparse.parse(sql_stmt)[0].tokens
        stmt_type = sqlparse.sql.Statement(parsed_stmt).get_type()
        identifier_list = []
        l = sqlparse.sql.IdentifierList(parsed_stmt).get_identifiers()
        for i in l:
            identifier_list.append(str(i))
        #print(identifier_list)
        if (stmt_type == 'SELECT'):
            try:
                ProcessSelectQuery(parsed_stmt, identifier_list)
            except:
                print("error: invalid query")
                sys.exit(1)
        else:
            print("error: invalid query")


def CheckTableExistence(table):
    if table in database_meta:
        return True
    else:
        return False

def ErrorInvalidTable():
    print("error: invalid table name")

def CheckFieldExistence(cols,table):
    valid_columns_flag = False
    for meta in cols:
        meta = meta.upper()
        if meta in database_meta[table]:
            valid_columns_flag = True
        else:
            valid_columns_flag = False
    return valid_columns_flag

def ErrorInvalidField():
    print("error: invalid field name")

def ErrorInvalidAggregateArguments():
    print("error: invalid number of aggregate arguments")

def ErrorInvalidAggregateType():
    print("error: invalid aggregate type")

def CheckAggregateFunction(columns):
    arg = columns[0]
    aggregate_type_and_col = []
    if re.match(r'MAX\(.+\)',arg):
        tp = re.sub(r'MAX\(','',arg)
        tp = re.sub(r'\)','',tp)
        aggregate_type_and_col.append(True)
        aggregate_type_and_col.append(tp.lower())
        aggregate_type_and_col.append("MAX")
    elif re.match(r'MIN\(.+\)',arg):
        tp = re.sub(r'MIN\(','',arg)
        tp = re.sub(r'\)','',tp)
        aggregate_type_and_col.append(True)
        aggregate_type_and_col.append(tp.lower())
        aggregate_type_and_col.append("MIN")
    elif re.match(r'AVG\(.+\)',arg):
        tp = re.sub(r'AVG\(','',arg)
        tp = re.sub(r'\)','',tp)
        aggregate_type_and_col.append(True)
        aggregate_type_and_col.append(tp.lower())
        aggregate_type_and_col.append("AVG")
    elif re.match(r'SUM\(.+\)',arg):
        tp = re.sub(r'SUM\(','',arg)
        tp = re.sub(r'\)','',tp)
        aggregate_type_and_col.append(True)
        aggregate_type_and_col.append(tp.lower())
        aggregate_type_and_col.append("SUM")
    
    return aggregate_type_and_col

def ProcessSelectQuery(parsed_stmt, identifier_list):
    query_executed_flag = False
    distinct_flag = 0
    if 'DISTINCT' in identifier_list:
        distinct_flag = 1

    # select max/min/avg(col1) from table1
    columns_index = []
    columns = identifier_list[-3].split(',')
    columns = [x.upper() for x in columns]
    check_table=True
    if ("," not in identifier_list[-1]):
        check_table=CheckTableExistence(identifier_list[-1])
    if  check_table== True:
        return_value = CheckAggregateFunction(columns)
        if len(return_value) == 0:
            pass
            #ErrorInvalidAggregateType()
        elif return_value[0] == True:
            columns = []
            columns.append(return_value[1].upper())
            if CheckFieldExistence(columns,identifier_list[-1]) == True:
                if len(columns) == 1:
                    columns_index.append(database_meta[identifier_list[-1]].index(columns[0]))
                    if return_value[2] == 'MAX':
                        star=database[identifier_list[-1]]
                        s=[]
                        mmax=-100000000
                        for val1 in star:
                            val2=[]
                            for i in columns_index:
                                val2.append(val1[i])
                            if(max(val2)>mmax):
                                mmax=max(val2)
                        print("max("+identifier_list[-1]+"."+columns[0]+")")
                        print(mmax)
                        query_executed_flag = True
                    elif return_value[2] == 'MIN':
                        star=database[identifier_list[-1]]
                        s=[]
                        mmin=100000000
                        for val1 in star:
                            val2=[]
                            for i in columns_index:
                                val2.append(val1[i])
                            if(min(val2)<mmin):
                                mmin=min(val2)
                        print("min("+identifier_list[-1]+"."+columns[0]+")")
                        print(mmin)
                        query_executed_flag = True
                    elif return_value[2] == 'AVG':
                        if distinct_flag == 0:
                            star=database[identifier_list[-1]]
                            s=[]
                            mmax=-100000000
                            for val1 in star:
                                val2=[]
                                for i in columns_index:
                                    val2.append(val1[i])
                                    s.append(val1[i])
                                if(max(val2)>mmax):
                                    mmax=max(val2)
                            print("avg("+identifier_list[-1]+"."+columns[0]+")")
                            print(sum(s)/len(s))
                        else:
                            star=database[identifier_list[-1]]
                            s=[]
                            for val1 in star:
                                val2=[]
                                for i in columns_index:
                                    if val1[i] not in s:
                                        s.append(val1[i])
                                    val2.append(val1[i])
                                #if val2 not in s:
                                    #s.append(val2)
                                    #print(str(val2)[1:-1])
                            print("avg("+identifier_list[-1]+"."+columns[0]+")")
                            print(sum(s)/len(s))

                        query_executed_flag = True
                    elif return_value[2] == 'SUM':
                        if distinct_flag == 0:
                            star=database[identifier_list[-1]]
                            s=[]
                            mmax=-100000000
                            for val1 in star:
                                val2=[]
                                for i in columns_index:
                                    val2.append(val1[i])
                                    s.append(val1[i])
                                if(max(val2)>mmax):
                                    mmax=max(val2)
                            print("sum("+identifier_list[-1]+"."+columns[0]+")")
                            print(sum(s))
                        else:
                            star=database[identifier_list[-1]]
                            s=[]
                            for val1 in star:
                                val2=[]
                                for i in columns_index:
                                    if val1[i] not in s:
                                        s.append(val1[i])
                                    val2.append(val1[i])
                                #if val2 not in s:
                                    #s.append(val2)
                                    #print(str(val2)[1:-1])
                            print("sum("+identifier_list[-1]+"."+columns[0]+")")
                            print(sum(s))
                        query_executed_flag = True
                elif len(columns) is not 1:
                    pass
                    #ErrorInvalidAggregateArguments()
            elif CheckFieldExistence(columns,identifier_list[-1]) == False:
                ErrorInvalidField()
    else:
        if "where" in query:
            pass
        else:
            ErrorInvalidTable()

    # select * from table1
    if query_executed_flag is not True:
        select_all_wildcard = sqlparse.sql.Identifier(parsed_stmt).is_wildcard()
        #print(select_all_wildcard, identifier_list)

        if select_all_wildcard == True:
            if CheckTableExistence(identifier_list[-1]) == True:
                if distinct_flag == 0:
                    star=database[identifier_list[-1]]
                    
                    a=""
                    for cols in database_meta[identifier_list[-1]]:
                        #print(cols)
                        a+=str(identifier_list[-1]+"."+cols+",")
                    a=a[0:len(a)-1]
                    print(a)
                    
                    for val1 in star:
                        print(str(tuple(val1))[1:-1])
                else:
                    star=database[identifier_list[-1]]
                    s=[]
                    for i in star:
                        if i not in s:
                            s.append(i)
                    print(star)
                    print(s)
                    a=""
                    for cols in database_meta[identifier_list[-1]]:
                        #print(cols)
                        a+=str(identifier_list[-1]+"."+cols+",")
                    a=a[0:len(a)-1]
                    print(a)
                    for val1 in s:
                        print(str(tuple(val1))[1:-1])
                    '''
                    star1=list(set(database[identifier_list[-1]]))
                    print(star1)
                    for val1 in star1:
                        print(str(tuple(val1))[1:-1])
                    '''
                query_executed_flag = True
            else:
                if "," not in identifier_list[-1]:
                    #print("aa")
                    if "where" not in query:
                        ErrorInvalidTable()
                else:
                    pass

    # select col1, col2 from table1
    if query_executed_flag is not True:
        where="where" in query
        #print(where)
        #print(query)
        if where==False:
            if ',' not in identifier_list[-1]:
                columns_index = []
                columns = identifier_list[-3].split(',')
                columns = [x.upper() for x in columns]
                #print(identifier_list,columns)

                if CheckTableExistence(identifier_list[-1]) == True:
                    if CheckFieldExistence(columns,identifier_list[-1]) == True:
                        for meta in columns:
                            columns_index.append(database_meta[identifier_list[-1]].index(meta))
                        
                        if distinct_flag == 0:
                            
                            star=database[identifier_list[-1]]
                            s=[]
                            a=""
                            #print(database_meta[identifier_list[-1]])
                            for cols in columns:
                                #print(cols)
                                a+=str(identifier_list[-1]+"."+cols+",")
                            a=a[0:len(a)-1]
                            print(a)
                            for val1 in star:
                                val2=[]
                                for i in columns_index:
                                    val2.append(val1[i])
                                print(str(val2)[1:-1])
                        else:
                            
                            star=database[identifier_list[-1]]
                            s=[]
                            a=""
                            for cols in columns:
                                #print(cols)
                                a+=str(identifier_list[-1]+"."+cols+",")
                            a=a[0:len(a)-1]
                            print(a)
                            for val1 in star:
                                val2=[]
                                for i in columns_index:
                                    val2.append(val1[i])
                                if val2 not in s:
                                    s.append(val2)
                                    print(str(val2)[1:-1])
                        query_executed_flag = True
                    elif CheckFieldExistence(columns,identifier_list[-1]) == False:
                        ErrorInvalidField()
                else:
                    #print("aa")
                    ErrorInvalidTable()

            elif ',' in identifier_list[-1]:
                columns_index = []
                columns = identifier_list[-3].split(',')
                columns = [x.upper() for x in columns]
                tables=identifier_list[-1].split(',')
                #print("aaaa")
                for table in tables:
                    if CheckTableExistence(table) == True:
                        if CheckFieldExistence(columns,table) == True:
                            for meta in columns:
                                columns_index.append(database_meta[table].index(meta))
                            #print(table,columns, columns_index)
                            if distinct_flag == 0:
                                
                                star=database[table]
                                s=[]
                                a=""
                                for cols in columns:
                                    #print(cols)
                                    a+=str(identifier_list[-1]+"."+cols+",")
                                a=a[0:len(a)-1]
                                new_columns=[]
                                #print(columns)
                                temp_flag=0
                                for col in columns:
                                    if col=="*":
                                        new_columns.append(col)
                                        temp_flag=1
                                    elif "." not in col:
                                        count=[]
                                        for table in tables:
                                            temp=table+"."+col
                                            if temp in schema[table]:
                                                count.append(table)
                                                new_columns.append(temp)
                                            col=count[0]+"."+col
                                            #print(temp)
                                    else:
                                        if len(col.split("."))==2:
                                            count=[]
                                            for table in tables:
                                                temp=col
                                                if temp in schema[table]:
                                                    count.append(table)
                                            if len(count)>1:
                                                print('Presence of conflicting columns')
                                                exit(0)
                                            else:
                                                col=col

                                        else:
                                            print("Something is wrong")
                                    if temp_flag!=1:
                                        pass
                                        #print(col)
                                        #new_columns.append(col)
                                #columns=new_columns
                                #print(new_columns)
                                for val in new_columns:
                                    if val!=new_columns[len(new_columns)-1]:
                                        print(val+",",end="")
                                    else:
                                        print(val)

                                for val1 in star:
                                    val2=[]
                                    for i in columns_index:
                                        val2.append(val1[i])
                                    print(str(val2)[1:-1])

                            else:
                                
                                star=database[table]
                                s=[]
                                a=""
                                for cols in columns:
                                    #print(cols)
                                    a+=str(identifier_list[-1]+"."+cols+",")
                                a=a[0:len(a)-1]
                                print(a)
                                for val1 in star:
                                    val2=[]
                                    for i in columns_index:
                                        val2.append(val1[i])
                                    if val2 not in s:
                                        s.append(val2)
                                        print(str(val2)[1:-1])
                            query_executed_flag = True
                            #continue
                        elif CheckFieldExistence(columns,table) == False:
                            ##print("a") 
                            cols1=[]
                            for table in tables:
                                for val in database_meta[table]:
                                    if val not in cols1:
                                        cols1.append(val)
                            #print(cols1)
                            sep={}
                            for table in tables:
                                sep[table]=[]
                            for val in cols1:
                                for table in tables:
                                    #print(val,table)
                                    if CheckFieldExistence(val,table):
                                        #print(val)
                                        sep[table].append(database_meta[table].index(val))
                                        break
                                        #cols.remove(val)
                            tabs=tables
                            cols=columns
                            if(cols==["*"]):
                                cols.remove("*")
                                for table in tables:
                                    for val in database_meta[table]:
                                        #if val not in cols:
                                        cols.append(val)
                            
                            #cols=list(set(cols))
                            #print(cols)
                            new_db={}
                            for data in tabs:
                                new_db[data]=[]
                            for data in cols:
                                dl=0
                                for data1 in tabs:
                                    if data in database1[data1][0]:
                                        if database1[data1][0].index(data) not in new_db[data1]:
                                            new_db[data1].append(database1[data1][0].index(data))
                                            dl=1
                                            break
                                        else:
                                            continue
                                if(dl==0):
                                    ErrorInvalidField()
                                    return
                            k9=0
                            l6=[]
                            for key1 in new_db.keys():
                                lo= new_db[key1]
                                l7=[]
                                for i9 in range(1,len(database1[key1])):
                                    lis=[]
                                    for i8 in lo:
                                        lis.append(database1[key1][i9][i8])
                                    l7.append(lis)
                                if(k9==0):
                                    l6=l7
                                else:
                                    l10=[]
                                    for i8 in range(len(l7)):
                                        l11=copy.deepcopy(l6)
                                        for mm in l11:
                                            mm.extend(l7[i8])
                                        l10.extend(l11)
                                    l6=l10
                                k9+=1
                            a=""
                            for key1 in new_db.keys():
                                for ll1 in new_db[key1]:
                                    #print(a)
                                    a+=str(key1+"."+database1[key1][0][ll1]+",")
                            a=a[0:len(a)-1]
                            print(a)
                            ##print()
                            kk=0
                            for key1 in l6:
                                #print(kk,",",end="")
                                for ll1 in key1:
                                    if(ll1!=key1[len(key1)-1]):
                                        print(ll1,",",end="")
                                    else:
                                        print(ll1,end="")
                                kk+=1
                                
                                print()
                            query_executed_flag = True
                        else:
                            ErrorInvalidField()
                    
                    else:
                        #print("aa")
                        ErrorInvalidTable()
                    break 

        else:
            if query[len(query)-1]!=';':
                print("Invalid Query Syntax")
                exit(0)
            else:
                query1=query.replace(';','')
            cond_flag=0
            conditionals=[]
            tables=[]
            columns=[]
            formatted_query=sqlparse.format(query1,reindent=True,keyword_case='upper')
            for parts in formatted_query.split("\n"):
                sub_query=parts.split()
                if len(sub_query)>=2:
                    if sub_query[0] in ("SELECT", "FROM", "WHERE"):
                        stage=sub_query[0]
                
                if stage=="SELECT":
                    if sub_query[0]=="SELECT":
                        for element in sub_query:
                            if str(element)=="DISTINCT":
                                distinct_flag=1
                                break
                            else:
                                distinct_flag=0
                    comma_removed_subpart=sub_query[-1].strip(",")
                    columns.append(remove_quote(comma_removed_subpart))
                
                if stage=="FROM":
                    #print sub_query[0]
                    if sub_query[0]=="FROM":
                        temp=sub_query[1]
                        for z in range(2,len(sub_query)):
                            temp+=" "+sub_query[z]
                        table_info=temp.strip(",")
                        tables.append(remove_quote(table_info))
                    else:
                        temp=sub_query[0]
                        for z in range(1,len(sub_query)):
                            temp+=" "+sub_query[z]
                        table_info=temp.strip(",")                        
                        tables.append(remove_quote(table_info))

                elif stage=="WHERE":
                    if sub_query[0]=="WHERE":
                        temp=sub_query[1]
                        for z in range(2,len(sub_query)):
                            temp+=""+sub_query[z]
                        conditionals.append(temp)
                    else:
                        temp=sub_query[0]
                        for z in range(1,len(sub_query)):
                            temp+=" "+sub_query[z]
                        conditionals.append(temp)
            #print(conditionals)
            #print(tables)
            #print(columns)
            #print()
            if conditionals:
                cond_flag=1
                temp=conditionals[0]
                for z in range(1,len(conditionals)):
                    temp+=" "+conditionals[z]
                text=temp
                pattern=re.compile(r'(\(|\))')
                substituted_conditionals=re.sub(pattern,r' \1 ',text)
                conditionals=substituted_conditionals.split()
                pattern2=re.compile(r'(=|!=|>|<|<=|>=)')
                temp=[]
                for i in conditionals:
                    if i not in COND_VALUES:
                        text2=i
                        sub_cond=re.sub(pattern2,r' \1 ',text2) 
                        cond=tuple(sub_cond.split())
                    else:
                        cond=i
                    temp.append(cond)
                conditionals=temp
            #print(conditionals)
            temp_flag=0
            new_columns=[]
            #print(schema)
            for col in columns:
                if col=="*":
                    new_columns.append(col)
                    temp_flag=1
                elif "." not in col:
                    count=[]
                    for table in tables:
                        temp=table+"."+col
                        if temp in schema[table]:
                            count.append(table)
                    if len(count)>1: 
                        print("conflicting Columns")
                        exit(0)
                    else:
                        col=count[0]+"."+col
                else:
                    if len(col.split("."))==2:
                        count=[]
                        for table in tables:
                            temp=col
                            if temp in schema[table]:
                                count.append(table)
                        if len(count)>1:
                            print("conflicting Columns")
                            exit(0)
                        else:
                            col=col
                    else:
                        print("Something is wrong")
                if temp_flag!=1:
                    new_columns.append(col)
            columns=new_columns
            new_schema=[]
            for tab in tables:
                new_schema.append(schema[tab])
            new_dataset=[{}]
            for table in tables:
                dataset2=[]
                for x in dataset[table]:
                    for y in new_dataset:
                        z={}
                        z.update(x)
                        z.update(y)
                        dataset2.append(z)
                new_dataset=dataset2
            final_dataset=[]
            for row in new_dataset:
                new_cond=[]
                for condition in conditionals:
                    if condition in COND_VALUES:
                        new_cond.append(condition.lower())
                    else:
                        if len(list(condition))>3:
                            operator=list(condition)[1]+""+list(condition)[2]
                        elif len(list(condition))==3:
                            operator=list(condition)[1]
                        else:
                            print("Something is wrong")
                        table_column=list(condition)[0]
                        val=list(condition)[len(list(condition))-1]
                        table_column=check_col_table(table_column,tables)
                        try:
                            val=int(remove_quote(val))
                        except ValueError: 
                            tab2=check_col_table(val,tables)
                            val=row[tab2]

                        if (operator=="=" and row[table_column]==val) or \
                        (operator==">" and row[table_column]>val) or \
                        (operator=="<" and row[table_column]<val) or \
                        (operator=="!=" and row[table_column]!=val) or \
                        (operator=="<=" and row[table_column]<=val) or \
                        (operator==">=" and row[table_column]>=val):
                            new_cond.append("True")
                        else:
                            if operator not in OP_VALUES:
                                print("invalid operator")
                                return
                            new_cond.append("False")
                flag="True"
                if len(new_cond)>0:
                    boolean_exp=new_cond[0]
                    for z in range(1,len(new_cond)):
                        boolean_exp+=" "+new_cond[z]
                    bool_val=str(eval(boolean_exp))
                else:
                    bool_val="True"
                if bool_val==flag:
                   final_dataset.append(row)
            names=[]
            if "*" in columns:
                for val in new_schema:
                    for i in val:
                        names.append(i)
            else:
                for col in columns:
                    if col!="*":
                        names.append(col)
            #print(names)
            #print(operator)
            if operator=="=":
                for val in names:
                    for val1 in names:
                        if val!=val1:
                            if val.split(".")[1]==val1.split(".")[1]:
                                names.remove(val1)
            #print(names) 
            distinct_rows=[]
            all_rows=[]
            for row in final_dataset:
                temp=[]
                for field in names:
                    temp.append(row[field])
                all_rows.append(temp)
                if temp not in distinct_rows:
                    distinct_rows.append(temp)
            if distinct_flag!=1:
                #print(all_rows)
                for val in names:
                    if val!=names[len(names)-1]:
                        print(val+",",end="")
                    else:
                        print(val)
                for i in all_rows:
                    print(str(i)[1:-1])
            else:
                #print(distinct_rows)
                for val in names:
                    if val!=names[len(names)-1]:
                        print(val+",",end="")
                    else:
                        print(val)
                for i in distinct_rows:
                    print(str(i)[1:-1])

            if ',' not in identifier_list[-1]:
                columns_index = []
                columns = identifier_list[-3].split(',')
                columns = [x.upper() for x in columns]
                if CheckTableExistence(identifier_list[-1]) == True:
                    if CheckFieldExistence(columns,identifier_list[-1]) == True:
                        for meta in columns:
                            columns_index.append(database_meta[identifier_list[-1]].index(meta))
                        
                        if distinct_flag == 0:
                            
                            star=database[identifier_list[-1]]
                            s=[]
                            for val1 in star:
                                val2=[]
                                for i in columns_index:
                                    val2.append(val1[i])
                                print(str(val2)[1:-1])
                        else:    
                            star=database[identifier_list[-1]]
                            s=[]
                            for val1 in star:
                                val2=[]
                                for i in columns_index:
                                    val2.append(val1[i])
                                if val2 not in s:
                                    s.append(val2)
                                    print(str(val2)[1:-1])
                        query_executed_flag = True
                    elif CheckFieldExistence(columns,identifier_list[-1]) == False:
                        ErrorInvalidField()
                else:
                    pass
                    #ErrorInvalidTable()

            elif ',' in identifier_list[-1]:
                columns_index = []
                columns = identifier_list[-3].split(',')
                columns = [x.upper() for x in columns]
                
                tables=identifier_list[-1].split(',')
                for table in tables:
                    if CheckTableExistence(table) == True:
                        if CheckFieldExistence(columns,table) == True:
                            for meta in columns:
                                columns_index.append(database_meta[table].index(meta))
                            
                            if distinct_flag == 0:
                                
                                star=database[table]
                                s=[]
                                for val1 in star:
                                    val2=[]
                                    for i in columns_index:
                                        val2.append(val1[i])
                                    print(str(val2)[1:-1])

                            else:
                                star=database[table]
                                s=[]
                                for val1 in star:
                                    val2=[]
                                    for i in columns_index:
                                        val2.append(val1[i])
                                    if val2 not in s:
                                        s.append(val2)
                                        print(str(val2)[1:-1])
                            query_executed_flag = True
                            #continue
                        elif CheckFieldExistence(columns,table) == False:
                             
                            cols=[]
                            for table in tables:
                                for val in database_meta[table]:
                                    if val not in cols:
                                        cols.append(val)
                            print(cols)
                            sep={}
                            for table in tables:
                                sep[table]=[]
                            for val in cols:
                                for table in tables:
                                    #print(val,table)
                                    if CheckFieldExistence(val,table):
                                        #print(val)
                                        sep[table].append(database_meta[table].index(val))
                                        break
                                        #cols.remove(val)
                            print(sep)
                            #keys=list(sep.keys)
                            print(database1)
                            
                            #pass

                            
                        else:
                            ErrorInvalidField()
                    
                    else:
                        #print("aa")
                        ErrorInvalidTable()
                    break


def ProcessMetaData():
    meta_data_file = open('metadata.txt','r')
    table_found = False
    table_name_found = False
    for meta in meta_data_file:
        if meta.strip() == "<begin_table>":
            table_found = True
            table_name_found = True

        elif table_found == True and table_name_found == True:
            meta = meta.strip()
            database_meta[meta] = []
            schema[meta]=[]
            table_name = meta
            table_name_found = False

        elif meta.strip() != "<end_table>":
            database_meta[table_name].append(meta.strip())
            schema[table_name].append(table_name+"."+meta.strip())

        elif meta.strip() == "<end_table>":
            table_found = False

    
def ProcessData():
    for meta_key, meta_value in database_meta.items():
        database[meta_key] = []
        data_table2=[]
        data_table3=[]
        data_table3.append(meta_value)
        file_name = meta_key + '.csv'
        with open(file_name) as f:
            for line in f:
                line=line.replace('\n', '', 1)
                val=list(line.split(","))
                for i in range(len(val)):
                    val[i]=int(val[i])
                data_table2.append(val)
                data_table3.append(val)
                split_row=line.split(",")
                tablename=file_name.split(".")[0]
                each_row_info=zip(schema[tablename],split_row)
                values=[]
                columns=[]
                for i in each_row_info:
                    j=list(i)
                    if len(remove_quote(j[1]))>0:
                        values.append(int(remove_quote(j[1])))
                        columns.append(j[0])
                    else: 
                        values.append(0) 
                        columns.append(j[0])
                temp_dict={}
                for x in range(0,len(columns)):
                    temp_dict[columns[x]]=values[x]
                dataset[tablename].append(temp_dict)
        database[meta_key] = (data_table2)
        database1[meta_key]=(data_table3)
    

if __name__ == "__main__":
    main()
