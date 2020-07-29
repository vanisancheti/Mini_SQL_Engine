import sys
from collections import OrderedDict

data_set = OrderedDict()
aggregrate_function = ['sum', 'max', 'min', 'avg']

def open_file(file):
    try:
        with open(file,'r')as f:
            data = [[line.replace('\n','')] for line in f]
    except:
        print("Error in opening file")
        sys.exit(1)

    return data

def open_csv_file(file):
    try:
        with open(file,'r')as f:
            data = [list(map(int, line.replace('\n','').replace('"','').split(','))) for line in f]
    except:
        print("Error in opening csv files")
    return data

def get_table_from_attribute(attribute, tables, wildcard_clause):

    attribute_temp = OrderedDict()
    for table in tables:
        attribute_temp[table] = []

    if(wildcard_clause):
        for table in tables:
            try:
                temp = list(data_set[table].keys())
            except:
                print("Invalid Statement: " + table + " not in the database")
                sys.exit(1)
            temp.remove("no_of_rows")
            attribute_temp[table].extend(temp)
    else:
        for att in attribute:
            dot_form = '.' in ' '.join(str(att))
            dot_index = -1

            table_name = []
            
            if dot_form:
                dot_index = att.index('.')
                if att[:dot_index] in tables:
                    table_name = [att[:dot_index]]
                    try:
                        a = data_set[table_name[0]][att[dot_index+1:]]
                    except:
                        print("Invalid Statement: "+ table_name[0] + " doesn't contain "+ att[dot_index+1:] + " attribute")
                        sys.exit(1)
            else:   
                for table in tables:
                    try:
                        temp = list(data_set[table].keys())
                    except:
                        print("Invalid Statement: " + table + " not in the database")
                        sys.exit(1)
                    
                    if att[dot_index+1:] in data_set[table].keys():
                        table_name.append(table)

            if len(table_name) > 1:
                print("Invalid Statement: Attribute "+ att[dot_index+1:] + " is ambigous")
                sys.exit(1)
            if len(table_name) == 0:
                print("Invalid Statement: Attribute "+ att[dot_index+1:] + " is not present in the given tables")
                sys.exit(1)

            column_name = att[dot_index+1:]
            attribute_temp[table_name[0]].append(column_name)

    return attribute_temp

def parse_condition(query):
    where_clause = 'where' in ' '.join(str(x.lower()) for x in query)
    if not where_clause:
        return None
    try:
        where_index = [x.lower() for x in query].index('where')
        condition = query[where_index+1:]
    except:
        print("Wrong Format: 'where'")
        sys.exit(1)

    condition_function_temp = OrderedDict()
    and_clause = 'and' in ' '.join(str(x.lower()) for x in condition)
    or_clause = 'or' in ' '.join(str(x.lower()) for x in condition)
    
    index = len(condition)

    if and_clause:
        try:
            and_index = [x.lower() for x in condition].index('and')
            index = and_index
            condition_function_temp['and'] = condition[and_index+1:]
            t = condition_function_temp['and'][2]
        except:
            print("Invalid Statement: Condition not properly formatted "+ ' '.join(str(x) for x in condition_function_temp))
            sys.exit(1)
    elif or_clause:
        try:
            or_index = [x.lower() for x in condition].index('or')
            index = or_index
            condition_function_temp['or'] = condition[or_index+1:]
            t = condition_function_temp['or'][2]
        except:
            print("Invalid Statement: Condition not properly formatted "+ ' '.join(str(x) for x in condition_function_temp))
            sys.exit(1)

    condition_function_temp['where'] = condition[:index]
    try:
        t = condition_function_temp['where'][2]
    except:
        print("Invalid Statement: Condition not properly formatted "+ ' '.join(str(x) for x in condition_function_temp))
        sys.exit(1)

    return condition_function_temp
    
def parse_attribute(attribute, tables):
    wildcard_clause = '*' in attribute
    if wildcard_clause and len(attribute) > 1 and 'distinct' not in [x.lower() for x in attribute]:
        print("Wrong Format")
        sys.exit(1)


    distinct_clause = 'distinct' in ' '.join(str(x.lower()) for x in attribute)
    max_clause = 'max' in ' '.join(str(x.lower()) for x in attribute)
    min_clause = 'min' in ' '.join(str(x.lower()) for x in attribute)
    sum_clause = 'sum' in ' '.join(str(x.lower()) for x in attribute)
    avg_clause = 'avg' in ' '.join(str(x.lower()) for x in attribute)

    aggregrate_function_temp = OrderedDict()
    condition_function_temp = OrderedDict()
    attribute_temp = OrderedDict()

    for table in tables:
        attribute_temp[table] = []

    if(distinct_clause):
        try:
            distinct_index = [x.lower() for x in attribute].index('distinct')
        except:
            print("Wrong Format of 'distinct' ")
            sys.exit(1)
        
        attribute = attribute[distinct_index+1:]
        condition_type = 'distinct'
        condition_function_temp[condition_type] = attribute


    if(max_clause or min_clause or sum_clause or avg_clause):
        try:
            open_paranthesis = attribute[0].index('(')
            close_paranthesis = attribute[0].index(')')
        except:
            print("Wrong Format")
            sys.exit(1)

        attribute = attribute[0][open_paranthesis+1:close_paranthesis]

        agg_type = None
        if max_clause:
            agg_type = 'max'
        elif min_clause:
            agg_type = 'min'
        elif sum_clause:
            agg_type = 'sum'
        elif avg_clause:
            agg_type = 'avg'

        for key, value in get_table_from_attribute([attribute], tables, False).items():
            if value:
                attribute_temp[key].extend(value)
                aggregrate_function_temp[agg_type] = value
        
    else:
        attribute_temp = get_table_from_attribute(attribute, tables, wildcard_clause)

    return [attribute_temp, aggregrate_function_temp, condition_function_temp]

def parse_string(query):
    table = []
    if ';' not in query:
        print("Invalid Format Missing ';'")
        sys.exit(1)

    semi_colon_index = query.index(';')
    query = query[:semi_colon_index].replace(',',' ').split()

    where_clause = 'where' in [x.lower() for x in query]
    
    try:
        i = [x.lower() for x in query].index('from')
    except:
        print("Invalid Query")
        sys.exit(1)

    if(where_clause):
        table = query[i+1:[x.lower() for x in query].index('where')]
    else:
        table = query[i+1:]

    try:
        select_index = [x.lower() for x in query].index('select')
        from_index = [x.lower() for x in query].index('from')
    except:
        print("Invalid Query")
        sys.exit(1)

    for t in table:
        try:
            a = data_set[t]
        except:
            print("Invalid Statement: " + t + " is not in the database")
            sys.exit(1)  

    if not table:
        print("Invalid Query: Missing Table")
        sys.exit(1)

    attribute = query[select_index+1:from_index]

    attribute_requested = parse_attribute(attribute, table)
    condition_requested = parse_condition(query)
    return [table, attribute_requested[0], attribute_requested[1], attribute_requested[2], condition_requested]

def sql_cross_product(tables, attributes, table_1, table_2, where_conditions=None, distinct_condition=None):
    cross_product_data = []
    join_condition = []
    filter_condition = []
    reversed_table = 0

    if where_conditions and len(where_conditions[0].keys()) >1:
        join_condition = where_conditions[0:2]
    elif where_conditions and len(where_conditions[2].keys()) >1:
        join_condition = where_conditions[2:4]
    if where_conditions and len(where_conditions[0].keys()) == 1:
        filter_condition = where_conditions[0:2]
    elif where_conditions and len(where_conditions[2].keys()) == 1:
        filter_condition = where_conditions[2:4]

    if join_condition and join_condition[0][0] != tables[0]:
        tables.reverse()
        reversed_table = 1
    final_indexes = []

    for i in table_1['indexes']:
        if not table_2:
            if join_condition and join_condition[1][1] == '=':
                join_condition[1][1] = '=='
            if not join_condition or (join_condition and eval("%s%s%s" % (data_set[join_condition[0][0]][join_condition[1][0]][i], join_condition[1][1], data_set[join_condition[0][2]][join_condition[1][2]][i]))):
                final_indexes.append([i])

        for j in table_2['indexes']:
            if join_condition and join_condition[1][1] == '=':
                join_condition[1][1] = '=='
            if not join_condition or (join_condition and eval("%s%s%s" % (data_set[join_condition[0][0]][join_condition[1][0]][i], join_condition[1][1], data_set[join_condition[0][2]][join_condition[1][2]][j]))):
                final_indexes.append([i, j])

    if where_conditions and where_conditions[4] == 'or':
        final_indexes_cond = []
        a = table_index(filter_condition[0][0], filter_condition[1][0], filter_condition)['indexes']
        if filter_condition[0][0] == tables[0]:
            for i in a:
                for j in table_2['indexes']:    
                    final_indexes_cond.append([i, j])
        else:
            for i in table_1['indexes']:
                for j in a:    
                    final_indexes_cond.append([i, j])
        final_indexes = [list(item) for item in set(tuple(row) for row in final_indexes_cond+final_indexes)]
        filter_condition = []
    
    for ind in final_indexes:
        temp_1 = []
        temp_2 = []
        if filter_condition:
            if filter_condition[0][0] == tables[0] and eval("%s%s%s" % (data_set[tables[0]][filter_condition[1][0]][ind[0]],filter_condition[1][1],filter_condition[1][2])):
                for att in attributes[tables[0]]:
                    temp_1.append(data_set[tables[0]][att][ind[0]])

                if len(ind) > 1:
                    for att in attributes[tables[1]]:
                        temp_2.append(data_set[tables[1]][att][ind[1]])
            elif len(ind) > 1 and filter_condition[0][0] == tables[1] and eval("%s%s%s" % (data_set[tables[1]][filter_condition[1][0]][ind[1]],filter_condition[1][1],filter_condition[1][2])):
                for att in attributes[tables[1]]:
                    temp_2.append(data_set[tables[1]][att][ind[1]])
            else:
                continue
        else:
            for att in attributes[tables[0]]:
                temp_1.append(data_set[tables[0]][att][ind[0]])
                
            if len(ind) > 1:
                for att in attributes[tables[1]]:
                    temp_2.append(data_set[tables[1]][att][ind[1]])

        if reversed_table == 1:
            cross_product_data.append(temp_2+temp_1)
        else:
            cross_product_data.append(temp_1+temp_2)

    if distinct_condition:
        set_cross_product_data = []
        for row in cross_product_data:
            if row not in set_cross_product_data:
                set_cross_product_data.append(row)

        cross_product_data = set_cross_product_data
    return cross_product_data

def table_index(table, attribute, where_conditions=None):
    if where_conditions is not None:
        indexes = range(0,data_set[table]['no_of_rows'])
        if 'or' in where_conditions:
            index_temp_1 = []
            index_temp_2 = []

            if where_conditions[1][1] == '=' or where_conditions[3][1] == '=':
                where_conditions[1][1] = '=='
                where_conditions[3][1] = '='

            for index in indexes:
                try:
                    if where_conditions[0][0] == table and eval("%s%s%s" % (data_set[where_conditions[0][0]][where_conditions[1][0]][index],where_conditions[1][1],where_conditions[1][2])):
                        index_temp_1.append(index)
                    if where_conditions[2][0] == table and eval("%s%s%s" % (data_set[where_conditions[2][0]][where_conditions[3][0]][index],where_conditions[3][1],where_conditions[3][2])):
                        index_temp_2.append(index)
                except:
                    print("Invalid Condition Format")
                    sys.exit(1)

            # for index in indexes:
            #     if where_conditions[1][1] == '=' or where_conditions[3][1] == '=':
            #         where_conditions[3][1] = '=='
            #         where_conditions[1][1] = '='
            #     try:
            #         if where_conditions[0][0] == table and eval("%s%s%s" % (data_set[where_conditions[0][0]][where_conditions[1][0]][index],where_conditions[1][1],where_conditions[1][2])):
            #             index_temp_2.append(index)
            #         if where_conditions[2][0] == table and eval("%s%s%s" % (data_set[where_conditions[2][0]][where_conditions[3][0]][index],where_conditions[3][1],where_conditions[3][2])):
            #             index_temp_2.append(index)
            #     except:
            #         print("Invalid Condition Format")
            #         sys.exit(1)

            indexes = list(set(index_temp_1 + index_temp_2))
            return {'indexes': indexes, 'agg_value': None} 
        else:
            index_temp_1 = []
            index_temp_2 = []

            if len(where_conditions[0].keys()) > 0 and where_conditions[0][0] == table:
                if where_conditions[1][1] == '=':
                    where_conditions[1][1] = '=='

                for index in indexes:
                    if eval("%s%s%s" % (data_set[where_conditions[0][0]][where_conditions[1][0]][index],where_conditions[1][1],where_conditions[1][2])):
                        index_temp_1.append(index)
            else:
                index_temp_1 = [x for x in range(0, data_set[table]['no_of_rows'])]

            if len(where_conditions) > 2 and len(where_conditions[2].keys()) > 0 and where_conditions[2][0] == table:
                if where_conditions[3][1] == '=':
                    where_conditions[3][1] = '=='
                for index in index_temp_1:
                    if where_conditions[2][0] == table and eval("%s%s%s" % (data_set[where_conditions[2][0]][where_conditions[3][0]][index],where_conditions[3][1],where_conditions[3][2])):
                        index_temp_2.append(index)
            else:
                index_temp_2 = index_temp_1

            indexes = index_temp_2
            return {'indexes': indexes, 'agg_value': None} 
    else:
        return {'indexes': [x for x in range(0, data_set[table]['no_of_rows'])],
                'agg_value': None}            

def aggregate_table_index(row, aggregate_condition):
    if not row:
        return [0]
    if 'max' in aggregate_condition.keys():
        return [max([r[0] for r in row])]

    if 'min' in aggregate_condition.keys():
        return [min([r[0] for r in row])]

    if 'sum' in aggregate_condition.keys():
        return [sum([r[0] for r in row])]

    if 'avg' in aggregate_condition.keys():
        return [sum([r[0] for r in row])/len(row)]

def print_output(columns, rows):
    print(','.join(str(x) for x in columns))
    for row in rows:
        print(','.join(str(x) for x in row))

def load_data():
    meta_data = open_file('metadata.txt')

    table_name = []
    attribute = []
    attribute_temp = []

    index = 0
    while index < len(meta_data):
        if meta_data[index][0] == '<end_table>':
            attribute.append(attribute_temp)
            index+=1
            continue
        if meta_data[index][0] == '<begin_table>':
            table_name.append(meta_data[index+1][0])
            index+=2
            attribute_temp = []
            continue
        attribute_temp.append(meta_data[index][0])
        index+=1

    for table in table_name:
        data_set_temp = OrderedDict()
        index = table_name.index(table) 
        data = open_csv_file(table+'.csv')
        attributes = attribute[index]
        data_set_temp['no_of_rows'] = len(data)

        for i in range(len(attributes)):
            data_set_temp[attributes[i]] = [row[i] for row in data]

        data_set[table] = data_set_temp

    return data_set

data_set = load_data()

query = sys.argv[1:]
query = query[0].replace('DISTINCT', 'distinct').replace('AND', 'and').replace('OR', 'or')
query_required = parse_string(query)
tables_required = query_required[0]
attributes_required = query_required[1]
aggregate_required = query_required[2]
distinct_required = query_required[3]
condition_required = query_required[4]


output_columns = []
output_rows = []

attribute_temp = None
cond_temp = []

if condition_required:
    cond_1 = condition_required['where']
    if cond_1[0].replace('-','').isdigit():
        cond_1.reverse()
        connection = cond_1[1] 
        if cond_1[1] == '<':
            connection = '>'
        if cond_1[1] == '>':
            connection = '<'
        if cond_1[1] == '<=':
            connection = '>='
        if cond_1[1] == '>=':
            connection = '<='

        cond_1[1] = connection

    table_1 = {cond_1.index(x): x.split('.')[0] for x in cond_1 if '.' in x}
    cond_1 = {cond_1.index(x): x.split('.')[-1] for x in cond_1}
    table_2 = OrderedDict()
    cond_2 = OrderedDict()
    connection = None

    if 'and' in condition_required.keys():
        cond_2 = condition_required['and']
        if cond_2[0].replace('-','').isdigit():
            cond_2.reverse()
            connection = cond_2[1] 
            if cond_2[1] == '<':
                connection = '>'
            if cond_2[1] == '>':
                connection = '<'
            if cond_2[1] == '<=':
                connection = '>='
            if cond_2[1] == '>=':
                connection = '<='

            cond_2[1] = connection
        table_2 = {cond_2.index(x): x.split('.')[0] for x in cond_2 if '.' in x}
        cond_2 = {cond_2.index(x): x.split('.')[-1] for x in cond_2}
        connection = 'and'
    elif 'or' in condition_required.keys():
        cond_2 = condition_required['or']
        if cond_2[0].replace('-','').isdigit():
            cond_2.reverse()
            connection = cond_2[1] 
            if cond_2[1] == '<':
                connection = '>'
            if cond_2[1] == '>':
                connection = '<'
            if cond_2[1] == '<=':
                connection = '>='
            if cond_2[1] == '>=':
                connection = '<='

            cond_2[1] = connection
        table_2 = {cond_2.index(x): x.split('.')[0] for x in cond_2 if '.' in x}
        cond_2 = {cond_2.index(x): x.split('.')[-1] for x in cond_2}
        connection = 'or'

    cond_temp = [table_1, cond_1, table_2, cond_2, connection]
    attribute_1 = OrderedDict()
    attribute_2 = OrderedDict()
    for table in tables_required:
        attribute_1[table] = []
        attribute_2[table] = []

    for keys in table_1.keys():
        attribute_1[table_1[keys]] = [cond_1[keys]]
        try:
            t = tables_required.index(table_1[keys])
        except:
            print("Invalid Statement: "+ table_1[keys] + " does not exit")
            sys.exit(1)
        try:
            a = data_set[table_1[keys]][cond_1[keys]]
        except:
            print("Invalid Statement: "+ table_1[keys] + " doesn't contain "+ cond_1[keys] + " attribute")
            sys.exit(1)

    if len(table_1.keys()) < len([cond_1[x] for x in cond_1.keys() if x != 1 and not cond_1[x].replace('-','').isdigit()]):
        attribute_1 = get_table_from_attribute([cond_1[x] for x in cond_1.keys() if x != 1 and not cond_1[x].replace('-','').isdigit()], tables_required, False)        
        
        for key in cond_1.keys():
            for table_key, value in attribute_1.items():
                if cond_1[key] in value:
                    table_1[key] = table_key

    if cond_2:
        for keys in table_2.keys():
            attribute_2[table_2[keys]] = [cond_2[keys]]
            try:
                t = tables_required.index(table_2[keys])
            except:
                print("Invalid Statement: "+ table_2[keys] + " does not exit")
                sys.exit(1)
            try:
                a = data_set[table_2[keys]][cond_2[keys]]
            except:
                print("Invalid Statement: "+ table_2[keys] + " doesn't contain "+ cond_2[keys] + " attribute")
                sys.exit(1)

        if len(table_2.keys()) < len([cond_2[x] for x in cond_2.keys() if x != 1 and not cond_2[x].replace('-','').isdigit()]):
            attribute_2 = get_table_from_attribute([cond_2[x] for x in cond_2.keys() if x != 1 and not cond_2[x].replace('-','').isdigit()], tables_required, False)

            for key in cond_2.keys():
                for table_key, value in attribute_2.items():
                    if cond_2[key] in value:
                        table_2[key] = table_key

    
    attribute_temp = {key: list(set(attribute_1.get(key, []) + attribute_2.get(key, []))) for key in set(attribute_1) | set(attribute_2)} # attribute_temp represent the attributes involved in all conditional part.

join_condition = []
if cond_temp and len(cond_temp[0].keys()) >1:
    join_condition = cond_temp[0:2]
elif cond_temp and len(cond_temp[2].keys()) >1:
    join_condition = cond_temp[2:4]


if join_condition and ('*' in query) and join_condition[1][1] == '=' and len(tables_required) > 1:
    attributes_required[join_condition[0][2]].remove(join_condition[1][2])

for table in tables_required:
    row = []
    if aggregate_required == OrderedDict():
        aggregate_required = None

    for att in attributes_required[table]:
        output_columns.append(str(table+"."+att))

    if condition_required:
        temp_cond = [condition_required[key][2].replace("-",'').isdigit() for key in condition_required.keys()]
        if False not in temp_cond:
            indexes = table_index(table, attribute_temp[table], where_conditions = cond_temp)
        else:
            indexes = table_index(table, attribute_temp[table])
    else:
        indexes = table_index(table, attributes_required[table])
    output_rows.append(indexes)

final_rows = output_rows[0]
if(len(output_rows)>1):
    for i in range(1,len(output_rows)):
        final_rows = sql_cross_product(tables_required[i-1:i+1], attributes_required, final_rows, output_rows[i], where_conditions = cond_temp , distinct_condition = distinct_required)
elif(join_condition):
        final_rows = sql_cross_product([tables_required[0]], attributes_required, final_rows, {}, where_conditions = cond_temp , distinct_condition = distinct_required)
else:
    output_rows = []
    if final_rows['indexes'] is not None:
        for i in final_rows['indexes']:
            temp_1 = []
            for att in attributes_required[tables_required[0]]:
                temp_1.append(data_set[tables_required[0]][att][i])
            output_rows.append(temp_1)

        if 'distinct'  in distinct_required.keys():
            set_row = [output_rows[0]]
            for r in output_rows:
                if r in set_row:
                    continue
                else:
                    set_row.append(r)

            output_rows = set_row
            
        final_rows = output_rows
    elif indexes['agg_value'] is not None:
        final_rows = [indexes['agg_value']]
    
if aggregate_required:
    for key in aggregate_required.keys():
        output_columns[0] = str(key) + '(' + output_columns[0] + ')'
    
    final_rows = [aggregate_table_index(final_rows, aggregate_condition = aggregate_required)]

print_output(output_columns, final_rows)
