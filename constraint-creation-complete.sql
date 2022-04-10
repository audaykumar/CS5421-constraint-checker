CREATE TABLE internal_constraints(
  ID SERIAL PRIMARY KEY,
  assertion_name varchar(50) NOT NULL,
  table_name           TEXT    NOT NULL,
  check_type            TEXT     NOT NULL,
  assertion            TEXT     NOT NULL
);

CREATE PROCEDURE create_assertion(assertion_name text, full_assertion text)
LANGUAGE plpython3u
AS $$
import re
split_assertion = re.split(r'\((.*)\)',full_assertion)
check_type = split_assertion[0].strip()
if check_type not in ["exists", "not exists"]:
	raise Exception(f"Syntax error: Please provide a exsists or not exists check condition")
assertion = split_assertion[1].strip()

execute_assertion = plpy.execute(f"select is_valid_syntax_check('{assertion}')")
return_message = (execute_assertion[0]["is_valid_syntax_check"]).split(":") 
is_valid = eval(return_message[0]) 
if not(is_valid): 
	raise Exception(f"Syntax error: '{return_message[1]}'")

tables_data = plpy.execute(f"select get_table_names('{assertion}')")
tables = tables_data[0]["get_table_names"]

for t_name in tables:
	query = f"Insert into internal_constraints (ASSERTION_NAME, TABLE_NAME, check_type, ASSERTION) values('{assertion_name}', '{t_name}', '{check_type}', '{assertion}')"
	plpy.info(query)
	try:
	  res = plpy.execute(query)
	except Exception as ex:
	  raise Exception(f"Error with assertion: '{ex}'")
$$;

-- is_valid_syntax_check: Checks if the assertion check syntax is a valid SQL syntax
CREATE OR REPLACE FUNCTION is_valid_syntax_check(assertion_str varchar(100))
  RETURNS VARCHAR
AS $$
	import sqlglot
	try:
		sqlglot.transpile(assertion_str)
	except Exception as ex:
		return "False:" + str(ex)
	return "True:"
$$ LANGUAGE plpython3u;

-- get_table_names: Gets the list of tables part of the check constraint to create a trigger for those tables
CREATE OR REPLACE FUNCTION get_table_names(query varchar(100))
  RETURNS VARCHAR[]
AS $$
  import itertools
  import sqlparse

  from sqlparse.sql import IdentifierList, Identifier
  from sqlparse.tokens import Keyword, DML


  def is_subselect(parsed):
    if not parsed.is_group:
      return False
    for item in parsed.tokens:
      if item.ttype is DML and item.value.upper() == 'SELECT':
        return True
    return False

  def extract_from_part(parsed):
    from_seen = False
    for item in parsed.tokens:
      if item.is_group:
        # print(item)
        for x in extract_from_part(item):
          yield x
      if from_seen:
        if is_subselect(item):
          for x in extract_from_part(item):
            yield x
        elif item.ttype is Keyword and item.value.upper() in ['ORDER', 'GROUP', 'BY', 'HAVING', 'GROUP BY']:
          from_seen = False
          StopIteration
        else:
          yield item
      if item.ttype is Keyword and item.value.upper() == 'FROM':
        from_seen = True


  def extract_table_identifiers(token_stream):
    for item in token_stream:
      if isinstance(item, IdentifierList):
        for identifier in item.get_identifiers():
          value = identifier.value.replace('"', '').lower()        
          yield value
      elif isinstance(item, Identifier):
        value = item.value.replace('"', '').lower()
        yield value

  def extract_tables(sql):
    extracted_tables = []
    statements = list(sqlparse.parse(sql))
    for statement in statements:
      # print(statement.tokens)
      if statement.get_type() != 'UNKNOWN':
        stream = extract_from_part(statement)
        extracted_tables.append(set(list(extract_table_identifiers(stream))))
    return list(itertools.chain(*extracted_tables))

  tables = extract_tables(query)
  return tables
  
$$ LANGUAGE plpython3u;

-- create_trigger: Uses the internal_constraints table to generate the trigger for each constraint
CREATE OR REPLACE FUNCTION create_trigger ()
RETURNS TRIGGER AS $$
  create_trigger_template = (
    "CREATE CONSTRAINT TRIGGER {table_name}_{assertion_name} "
    "AFTER INSERT OR UPDATE OR DELETE "
    "ON {table_name} DEFERRABLE INITIALLY DEFERRED FOR EACH ROW "
    "EXECUTE FUNCTION constraint_checker();"
  )
  plpy.execute(create_trigger_template.format(table_name=TD["new"]["table_name"], assertion_name=TD["new"]["assertion_name"]))
$$ LANGUAGE plpython3u;

CREATE OR REPLACE TRIGGER after_create_assertion
AFTER INSERT ON internal_constraints FOR EACH ROW
EXECUTE FUNCTION create_trigger();

CREATE OR REPLACE FUNCTION constraint_checker ()
  RETURNS trigger
AS $$
table_name = TD["table_name"]
query = f"Select assertion_name,check_type,assertion from internal_constraints where table_name='{table_name}'"
res = plpy.execute(query, 5)
nrows = res.nrows()
plpy.log(f"Total Assertions:{nrows}")
for i in range(nrows):
	assertion = res[i]["assertion"]
	assertion_type = res[i]["check_type"].strip().lower()
	assertion_name = res[i]["assertion_name"].strip().lower()
	query = f"SELECT {assertion_type}({assertion}) as result"
	plpy.log(f"**Verifying Assertion: {query}**")
	output = plpy.execute(query)
	plpy.log(output)
	if not output[0]["result"]:
		Constraint_Violation_Exception = Exception(f"Error: Assertion failed for {table_name} -> {assertion_name}")
		raise Constraint_Violation_Exception
plpy.log(f"All assertions passed successfully.")
return "OK"
$$ LANGUAGE plpython3u;