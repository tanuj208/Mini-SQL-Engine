from error import *
VALID_AGGREGATE_FUNCTIONS = ["max", "min", "sum", "avg"]
VALID_OPERATORS = ['<=', '>=', '<', '>', '=']

def filter_arr(arr, fn):
    if not arr:
        return arr
    if fn == 'max':
        return max(arr)
    elif fn == 'min':
        return min(arr)
    elif fn == 'sum':
        return sum(arr)
    elif fn == 'avg':
        return sum(arr)/len(arr)

def join_two_tables(tab1, tab2):
    ret_tab = [tab1[0] + tab2[0]]
    for i in range(1,len(tab1)):
        row = tab1[i]
        for j in range(1, len(tab2)):
            row2 = tab2[j]
            ret_tab.append(row+row2)
    return ret_tab

def process_condition(term):
    if not term:
        return term
    for oper in VALID_OPERATORS:
        if oper in term:
            term = term.split(oper)
            term.insert(1, oper)
            break
    if not isinstance(term, list):
        show_error(INVALID_WHERE_CONDITION)
    for i in range(len(term)):
        term[i] = term[i].strip()
        try:
            term[i] = int(term[i])
        except:
            continue
    return term

def check_condition(num1, num2, op):
    if op == '=':
        if num1 == num2:
            return True
        else:
            return False
    elif op == '>':
        if num1 > num2:
            return True
        else:
            return False
    elif op == '<':
        if num1 < num2:
            return True
        else:
            return False
    elif op == '>=':
        if num1 >= num2:
            return True
        else:
            return False
    elif op == '<=':
        if num1 <= num2:
            return True
        else:
            return False

class Query:
    def __init__(self, statement, database):
        self.statement = statement
        self.database = database
        self.query_params = {}
        self.final_ans = []
        self.same_cols = []

    def check_syntax_error(self):
        statement = list(self.statement)
        total_length = len(statement)

        if total_length == 0:
            show_error(INVALID_SYNTAX)
        if str(self.statement[0]) != 'SELECT':
            show_error(SELECT_ERROR)

        if total_length <= 2:
            show_error(INVALID_SYNTAX)
        if str(self.statement[2]) == 'DISTINCT':
            self.query_params['distinct'] = True
            from_ptr = 6
        else:
            from_ptr = 4

        if total_length <= from_ptr:
            show_error(INVALID_SYNTAX)            
        if str(statement[from_ptr]) != 'FROM':
            show_error(FROM_ERROR)

        last_q = str(statement[total_length-1])
        if last_q[len(last_q)-1] != ';':
            show_error(SEMICOLON_ERROR)

        from_ptr += 4
        if total_length > from_ptr:
            if str(statement[from_ptr][0]) != 'WHERE':
                show_error(INVALID_SYNTAX)
        return
    
    def ambiguity_check(self, col_name):
        if '.' in col_name:
            return
        occ = 0
        for table_name in self.query_params["table_names"]:
            if col_name in self.database.tableInfo[table_name]:
                occ += 1
        if occ > 1:
            show_error(AMBIGUITY_ERROR.format(col_name))
        return
    
    def get_column_names(self, project_query):
        col_names = project_query.split(',')
        self.query_params['aggregates'] = []
        self.query_params['col_names'] = []
        for i in range(len(col_names)):
            col = col_names[i].strip()
            if col == '*':
                self.query_params['all_cols'] = True
                return
            idx1 = col.find('(')
            idx2 = col.find(')')

            if idx1 != -1 and idx2 != -1:
                fn = col[0:idx1]
                col = col[idx1+1:idx2]
                if col == '*':
                    show_error(STAR_AGGREGATE_ERROR)
                if fn not in VALID_AGGREGATE_FUNCTIONS:
                    show_error(NO_AGGREGATE_FUNCTION_ERROR)
                self.query_params['aggregates'].append(fn)
            self.query_params['col_names'].append(col)
        if self.query_params['aggregates'] and len(self.query_params['aggregates']) != len(self.query_params['col_names']):
            show_error(AGGREGATE_FUNCTION_ERROR)
        return

    def get_table_names(self, select_query):
        self.query_params['table_names'] = select_query.split(',')
        tmp_table = []
        for i in range(len(self.query_params['table_names'])):
            self.query_params['table_names'][i] = self.query_params['table_names'][i].strip()
            if self.query_params['table_names'][i] in tmp_table:
                show_error(TABLE_REPEAT_ERROR)
            tmp_table.append(self.query_params['table_names'][i])
        return
    
    def join_all_tables(self):
        for name in self.query_params['table_names']:
            col_names = [name + '.' + s for s in self.database.tableInfo[name]]
            tmp_table = [col_names] + self.database.tables[name]
            if not self.final_ans:
                self.final_ans = tmp_table
            else:
                self.final_ans = join_two_tables(self.final_ans, tmp_table)
        return
    
    def get_where_attrs(self, where_query):
        comp1 = ""
        comp2 = ""
        join_cond = ""
        for i in range(1, len(list(where_query))):
            term = where_query[i]
            if type(term).__name__ == 'Comparison':
                if comp1:
                    comp2 = str(term)
                else:
                    comp1 = str(term)
                continue
            term = str(term)
            term = term.strip()
            if not term or term[0] == ';':
                continue
            if term == 'AND' or term == 'OR':
                join_cond = term
            else:
                show_error(INVALID_SYNTAX)
        comp1 = process_condition(comp1)
        comp2 = process_condition(comp2)
        if bool(join_cond) ^ bool(comp2):
            show_error(INVALID_WHERE_CONDITION)
        
        if comp1:
            if not isinstance(comp1[0], int):
                self.ambiguity_check(comp1[0])
            if not isinstance(comp1[2], int):
                self.ambiguity_check(comp1[2])
        if comp2:
            if not isinstance(comp2[0], int):
                self.ambiguity_check(comp2[0])
            if not isinstance(comp2[2], int):
                self.ambiguity_check(comp2[2])
        where_attrs = {}
        where_attrs['cmp1'] = comp1
        where_attrs['cmp2'] = comp2
        where_attrs['cond'] = join_cond
        self.query_params['where_attrs'] = where_attrs
        return
    
    def check_row_condition(self, row, col_names, cond):
        col1 = cond[0]
        col2 = cond[2]
        op = cond[1]
        same_col1 = ""
        same_col2 = ""
        if not isinstance(col1, int):
            if '.' not in col1:
                for table in self.database.tables:
                    full_name = table + '.' + col1
                    if full_name in col_names:
                        col1 = full_name
                        break
            if col1 not in col_names:
                show_error(NO_COLUMN_ERROR.format(col1))
            same_col1 = col1
            idx1 = col_names.index(col1)
            num1 = row[idx1]
        else:
            num1 = col1

        if not isinstance(col2, int):
            if '.' not in col2:
                for table in self.database.tables:
                    full_name = table + '.' + col2
                    if full_name in col_names:
                        col2 = full_name
                        break

            if col2 not in col_names:
                show_error(NO_COLUMN_ERROR.format(col2))
            same_col2 = col2
            idx2 = col_names.index(col2)
            num2 = row[idx2]
        else:
            num2 = col2

        same_cols = []
        if same_col1 and same_col2 and op == '=' and col1.split('.')[0] != col2.split('.')[0]:
            same_cols = [same_col1, same_col2]

        return check_condition(num1, num2, op), same_cols

    def process_where_clause(self):
        cmp1 = self.query_params['where_attrs']['cmp1']
        cmp2 = self.query_params['where_attrs']['cmp2']
        cond = self.query_params['where_attrs']['cond']

        new_data = [self.final_ans[0]]
        for i in range(1, len(self.final_ans)):
            if cmp1:
                bool1, same_col = self.check_row_condition(self.final_ans[i], self.final_ans[0], cmp1)
                if len(self.same_cols) < 2:
                    self.same_cols.append(same_col)
            if cmp2:
                bool2, same_col = self.check_row_condition(self.final_ans[i], self.final_ans[0], cmp2)
                if len(self.same_cols) < 2:
                    self.same_cols.append(same_col)

            if not cmp2 and not bool1:
                continue
            elif not cmp2:
                new_data.append(self.final_ans[i])
                continue
            if (cond == 'OR' and (bool1 or bool2)) or (cond == 'AND' and bool1 and bool2):
                new_data.append(self.final_ans[i])
        self.final_ans = new_data
        return

    def merge_col_in_table(self, table, column):
        col_idx = self.final_ans[0].index(column)
        if not table:
            table.append([column])
        else:
            table[0].append(column)
        for i in range(1, len(self.final_ans)):
            val = self.final_ans[i][col_idx]
            if len(table) <= i:
                table.append([val])
            else:
                table[i].append(val)        

    def filter_cols(self):
        filtered_table = []
        for column in self.query_params['col_names']:
            if '.' in column:
                table_name = column.split('.')[0]
                if table_name not in self.database.tableInfo:
                    show_error(NO_TABLE_ERROR.format(table_name))
                if column not in self.final_ans[0]:
                    show_error(NO_COLUMN_ERROR.format(column))
                self.merge_col_in_table(filtered_table, column)
            else:
                flag = True
                for col in self.final_ans[0]:
                    col2 = col.split('.')[1]
                    if col2 == column:
                        self.merge_col_in_table(filtered_table, col)
                        flag = False
                        break
                if flag:
                    show_error(NO_COLUMN_ERROR.format(column))
        self.final_ans = filtered_table
        
    def handle_aggregates(self):
        col_names = []
        for i in range(len(self.query_params['aggregates'])):
            col_names.append(self.query_params['aggregates'][i] + '(' + self.final_ans[0][i] + ')')
        filtered_table = [col_names]
        temp = []
        for i in range(len(self.query_params['aggregates'])):
            fn = self.query_params['aggregates'][i]
            cur_col = []
            for j in range(1, len(self.final_ans)):
                row = self.final_ans[j]
                cur_col.append(row[i])
            new_arr = filter_arr(cur_col, fn)
            if(new_arr):
                temp.append(new_arr)
        if temp:
            filtered_table.append(temp)
        self.final_ans = filtered_table
        return
    
    def process_distinct(self):
        distinct_data = []
        for row in self.final_ans:
            if row not in distinct_data:
                distinct_data.append(row)
        self.final_ans = distinct_data
        return
    
    def delete_same_cols(self):
        for same_col_pair in self.same_cols:
            if not same_col_pair:
                continue
            col1 = same_col_pair[0]
            col2 = same_col_pair[1]
            if col1 in self.final_ans[0] and col2 in self.final_ans[0]:
                idx = self.final_ans[0].index(col1)
                new_data = []
                for row in self.final_ans:
                    new_row = []
                    for j in range(len(row)):
                        if j != idx:
                            new_row.append(row[j])
                    new_data.append(new_row)
                self.final_ans = new_data
        return
    
    def print_output(self):
        for row in self.final_ans:
            print(','.join(map(str, row)))
        return
    
    def parse_statement(self):
        self.query_params['distinct'] = False
        self.query_params['all_cols'] = False
        self.check_syntax_error()

        if self.query_params['distinct']:
            cur_ptr = 4
        else:
            cur_ptr = 2

        self.get_column_names(str(self.statement[cur_ptr]))
        cur_ptr += 4
        self.get_table_names(str(self.statement[cur_ptr]))


        for table in self.query_params['table_names']:
            if table not in self.database.tableInfo:
                show_error(NO_TABLE_ERROR.format(table))

        for col_name in self.query_params['col_names']:
            self.ambiguity_check(col_name)

        cur_ptr += 2
        if len(list(self.statement)) > cur_ptr:
            self.get_where_attrs(self.statement[cur_ptr])
        return

    def query_statement(self):
        self.join_all_tables()
        if 'where_attrs' in self.query_params:
            self.process_where_clause()
        if not self.query_params['all_cols']:
            self.filter_cols()
        if self.query_params['aggregates']:
            self.handle_aggregates()
        if self.query_params['distinct']:
            self.process_distinct()
        if self.same_cols and self.query_params['all_cols']:
            self.delete_same_cols()
        return
