# Лабораторная работа 2

## Задание: 

Сформировать отчёт с информацией о 10 наиболее популярных языках программирования по итогам года за период с 2010 по 2020 годы. Отчёт будет отражать динамику изменения популярности языков программирования и представлять собой набор таблиц "топ-10" для каждого года.

Получившийся отчёт сохранить в формате Apache Parquet.

Для выполнения задания вы можете использовать любую комбинацию Spark API: RDD API, Dataset API, SQL API.
Работа начинается с инициализации сессии: 

### Выполнение начинаем с инициализации сессии: 

![1](https://github.com/vmokook/BigData/blob/main/LR2/Image/1.png)

### Далее загружаем файлы: 

А также посмотрим их структуру..
![2](https://github.com/vmokook/BigData/blob/main/LR2/Image/2.png) ![3](https://github.com/vmokook/BigData/blob/main/LR2/Image/3.png) 

### Работа над формированием таблицы: 

1. Будем исходить из того, что популярность языка программирвоания можно оценить по количеству просмотров поста (столбец "_ViewCount"), 
в столбце "_Tags" можно найти информацию о языке программирования, при выборе года будем ориентироваться на датуц создания поста (столбец "_CreationDate").
Выберем необходимые столбцы из исходной таблицы:
 
![4](https://github.com/vmokook/BigData/blob/main/LR2/Image/4.png) 

2. Выделим год из столбца "_CreationDate":

![5](https://github.com/vmokook/BigData/blob/main/LR2/Image/5.png) 

3. Удаляем все null значения из таблицы, а также избавляемся от столбца "_CreationDate": 

![6](https://github.com/vmokook/BigData/blob/main/LR2/Image/6.png) 

4. Оставляем только нужные нам года 2010-2020:

![7](https://github.com/vmokook/BigData/blob/main/LR2/Image/7.png) 

5. Далее из таблицы "programming-languages.csv" извлекаем информацию о названиях языков программирования. Это пригодится для фильтрации
значений в столбце "_Tags".

![8](https://github.com/vmokook/BigData/blob/main/LR2/Image/8.png) 

6. В нашей таблице формируется новый столбец "language", операция `regexp_extract('_Tags', r'<([^>]+)>', 1)` выполняет
извлечение содержимого внутри скобок '<...>' из столбца '_Tags'. Извлекается только 1-ое такое выражение. Затем столбец
'_Tags' удаляется `.drop('_Tags')`

![9](https://github.com/vmokook/BigData/blob/main/LR2/Image/9.png) 

7. Проверяем совпадение извлеченных выражений из '_Tags' с названиями языков программирования language. 

![10](https://github.com/vmokook/BigData/blob/main/LR2/Image/10.png) 

8. Группируем данные по годам и языкам программирования, вычисляем сумму просмотров и называем данный столбец "Popularity".

![11](https://github.com/vmokook/BigData/blob/main/LR2/Image/11.png) 

9. Задаем окно, в пределах которого проводится разбиение данных на группы по значению столбца 'Year'.
Сортируем данные по убыванию по столбцу "Popularity" внутри каждого окна. Каждой строке присваим ранг.
Далее фильтруем те строки, у которых ранг не выше 10. Таким образом, остаются только топ-10 языков
программирования для каждого года.

![12](https://github.com/vmokook/BigData/blob/main/LR2/Image/12.png) 

### Результат:

![13](https://github.com/vmokook/BigData/blob/main/LR2/Image/13.png) 
![14](https://github.com/vmokook/BigData/blob/main/LR2/Image/14.png) 
![15](https://github.com/vmokook/BigData/blob/main/LR2/Image/15.png) 


![16](https://github.com/vmokook/BigData/blob/main/LR2/Image/16.png) 

![17](https://github.com/vmokook/BigData/blob/main/LR2/Image/17.png) 
