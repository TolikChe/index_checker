require 'rubygems'
require 'nokogiri'
require 'oci8'

# Глобальные пременные
#
# Переменные подклчюения к схеме
$userName = 'CRM_31_CHE'
$userPassword = 'CRM_31_CHE'
$baseName = 'ntdb10'
#
# Файл содержищий информацию об индексах
$indexes_file_name = 'INDEXES.xml'

#
#
# Класс Индекс. 
# Содержит информацию об индексе (Название, имя таблицы, поля)
class Index
	attr_reader :index_name, :table_name, :columns, :exist 
	# конструктор
	def initialize (i_name, t_name, cols)
		@index_name, @table_name, @columns = i_name, t_name, cols
	end
	#
	def set_exist (exist)
		@exist = exist
	end
	#
	# функция генерит запрос на проверку того что индекс существует
	# Учитывается имя индекса, 
	#			  имя таблицы в котрой он находится, 
	#             имя полей, их которых он состоит
	#             позиция полей в индексе
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
# Класс Колонки
# Класс содержит название колонки и ее порядок в индексе
class Column
	attr_reader :column_name, :column_position 
	# конструктор
	def initialize (c_name, c_position)
		@column_name, @column_position = c_name, c_position
	end
end

#
# 
# Функция открывает xml фал с описанием нидексов и считыает его в массив индексов.
def get_indexes 
	# Открываем файл, считываем его в переменную типа xml
	@doc = Nokogiri::XML(File.read($indexes_file_name))
	# считываем через xpath содержимое тегов INDEX в массив
	@indexes = @doc.xpath('//index')
end

#
#
# Функция разбирает xml индекса и наполняет объект типа Index информацией
def parse_index ( index )
	# инициализируем массив
	@cols = Array.new()
	# Преобразуем строку в xml
	@index_xml = Nokogiri::XML(index.to_s)
	# Достаем из XML значения аттрибутов
	@index_name = @index_xml.xpath('//@index_name').to_s
	@table_name = @index_xml.xpath('//@table_name').to_s
	# Считываем масив тегов в массив
	@columns_list_xml = @index_xml.xpath('//column')
	# Разбираем каждый элемент массива отдельно
	@columns_list_xml.each { | column |
		@column_xml = Nokogiri::XML(column.to_s)
		# Вытаскиваем из XML нужные значения аттрибутов
		column_name = @column_xml.xpath('//@column_name').to_s
		column_position = @column_xml.xpath('//@column_position').to_s
		# Инициализируем объект типа колонка
		@col = Column.new(column_name, column_position)
		# Собираем массив значений
		@cols << @col
	}	
	# Инициализируем объект типа индекс
	@Index = Index.new( @index_name, @table_name, @cols )
end

#
#
# Распечатываем содержимое объекта типа Индекс
def print_index ( index )
	puts 'Index ' + index.index_name + ' on table ' + index.table_name
	index.columns.each { | index_column |
		puts '	column name: ' + index_column.column_name + ' position: ' + index_column.column_position
	}
end

#
#
# Выполним пришедший запрос с использованием переданного соединения
def exec_sql( connection, sql_text, index, i )
	# Выполним пришедший запрос с использованием переданного соединения
	cursor = connection.exec(sql_text)
	result = cursor.fetch()
	r = result[0].to_i
	# Если запрос вернул 1 то говорим что все ОК
	# В противном случае говорим что ошибка
	if r == 1
		puts "#{i+1}  OK #{index.index_name}"
	else
		puts "#{i+1} !!! ERROR " 
		puts print_index(index)
	end 
	cursor.close
end


# Подключаемся к указанной базе
db_connect = OCI8.new($userName,$userPassword,$baseName)

# Открываем файл с xml в котором лежит информация об индексах
#     Считываем из него информацию в массив
#     Получаем массив из строк вида:   
#		<index table_name="CMS_CF_REASON" index_name="CFRS_PK">
#			<columns>
#				<column column_name="CFRS_ID" column_position="1"/>
#			</columns>
#		</index>								
index_list_xml = get_indexes()
# Выводим общее число строк в массиве (Число индексов для разбора)
puts index_list_xml.count

# Каждый элемент массива разбираем и превращаем в объект типа Index
# Получаем массив объектов типа Index 
index_list = Array.new()
index_list_xml.each_with_index { | index_xml, i |
	# Разбираем каждый xml индекса и превращаем его в объект
	index = parse_index(index_xml)
	# Строим запрос для проверки существования индекса
	sql_text = index.gen_check_select
	# Выполняем построенный запрос
	exec_sql( db_connect, sql_text, index, i )
}
# Закрываем соединение с базой
db_connect.logoff