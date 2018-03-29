require 'rubygems'
require 'nokogiri'
require 'oci8'

# ���������� ���������
#
# ���������� ����������� � �����
$userName = 'CRM_31_CHE'
$userPassword = 'CRM_31_CHE'
$baseName = 'ntdb10'
#
# ���� ���������� ���������� �� ��������
$indexes_file_name = 'INDEXES.xml'

#
#
# ����� ������. 
# �������� ���������� �� ������� (��������, ��� �������, ����)
class Index
	attr_reader :index_name, :table_name, :columns, :exist 
	# �����������
	def initialize (i_name, t_name, cols)
		@index_name, @table_name, @columns = i_name, t_name, cols
	end
	#
	def set_exist (exist)
		@exist = exist
	end
	#
	# ������� ������� ������ �� �������� ���� ��� ������ ����������
	# ����������� ��� �������, 
	#			  ��� ������� � ������ �� ���������, 
	#             ��� �����, �� ������� �� �������
	#             ������� ����� � �������
	def gen_check_select 
		sql_text = ' select to_char(count(*)) from user_indexes i where i.table_name = \'' + @table_name + '\' and i.index_name = \'' + @index_name + '\' and (i.status = \'VALID\' or (i.status = \'N/A\' and partitioned = \'YES\') )'

		@columns.each { | column |
			sql_text << ' and exists (select 1 from user_ind_columns ic where ic.index_name = i.index_name  and ic.table_name = i.table_name  and ic.column_name = \'' + column.column_name + '\' and ic.column_position = ' + column.column_position + ') '
		}
		return sql_text
	end
end

#
#
# ����� �������
# ����� �������� �������� ������� � �� ������� � �������
class Column
	attr_reader :column_name, :column_position 
	# �����������
	def initialize (c_name, c_position)
		@column_name, @column_position = c_name, c_position
	end
end

#
# 
# ������� ��������� xml ��� � ��������� �������� � �������� ��� � ������ ��������.
def get_indexes 
	# ��������� ����, ��������� ��� � ���������� ���� xml
	@doc = Nokogiri::XML(File.read($indexes_file_name))
	# ��������� ����� xpath ���������� ����� INDEX � ������
	@indexes = @doc.xpath('//index')
end

#
#
# ������� ��������� xml ������� � ��������� ������ ���� Index �����������
def parse_index ( index )
	# �������������� ������
	@cols = Array.new()
	# ����������� ������ � xml
	@index_xml = Nokogiri::XML(index.to_s)
	# ������� �� XML �������� ����������
	@index_name = @index_xml.xpath('//@index_name').to_s
	@table_name = @index_xml.xpath('//@table_name').to_s
	# ��������� ����� ����� � ������
	@columns_list_xml = @index_xml.xpath('//column')
	# ��������� ������ ������� ������� ��������
	@columns_list_xml.each { | column |
		@column_xml = Nokogiri::XML(column.to_s)
		# ����������� �� XML ������ �������� ����������
		column_name = @column_xml.xpath('//@column_name').to_s
		column_position = @column_xml.xpath('//@column_position').to_s
		# �������������� ������ ���� �������
		@col = Column.new(column_name, column_position)
		# �������� ������ ��������
		@cols << @col
	}	
	# �������������� ������ ���� ������
	@Index = Index.new( @index_name, @table_name, @cols )
end

#
#
# ������������� ���������� ������� ���� ������
def print_index ( index )
	puts 'Index ' + index.index_name + ' on table ' + index.table_name
	index.columns.each { | index_column |
		puts '	column name: ' + index_column.column_name + ' position: ' + index_column.column_position
	}
end

#
#
# �������� ��������� ������ � �������������� ����������� ����������
def exec_sql( connection, sql_text, index, i )
	# �������� ��������� ������ � �������������� ����������� ����������
	cursor = connection.exec(sql_text)
	result = cursor.fetch()
	r = result[0].to_i
	# ���� ������ ������ 1 �� ������� ��� ��� ��
	# � ��������� ������ ������� ��� ������
	if r == 1
		puts "#{i+1}  OK #{index.index_name}"
	else
		puts "#{i+1} !!! ERROR " 
		puts print_index(index)
	end 
	cursor.close
end


# ������������ � ��������� ����
db_connect = OCI8.new($userName,$userPassword,$baseName)

# ��������� ���� � xml � ������� ����� ���������� �� ��������
#     ��������� �� ���� ���������� � ������
#     �������� ������ �� ����� ����:   
#		<index table_name="CMS_CF_REASON" index_name="CFRS_PK">
#			<columns>
#				<column column_name="CFRS_ID" column_position="1"/>
#			</columns>
#		</index>								
index_list_xml = get_indexes()
# ������� ����� ����� ����� � ������� (����� �������� ��� �������)
puts index_list_xml.count

# ������ ������� ������� ��������� � ���������� � ������ ���� Index
# �������� ������ �������� ���� Index 
index_list = Array.new()
index_list_xml.each_with_index { | index_xml, i |
	# ��������� ������ xml ������� � ���������� ��� � ������
	index = parse_index(index_xml)
	# ������ ������ ��� �������� ������������� �������
	sql_text = index.gen_check_select
	# ��������� ����������� ������
	exec_sql( db_connect, sql_text, index, i )
}
# ��������� ���������� � �����
db_connect.logoff