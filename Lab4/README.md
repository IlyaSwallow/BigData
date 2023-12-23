# Лабораторная работа № 4

## 1) Работа с Zookeeper через консоль

 - Сначала про работу в консоли.

 - Посмотрели содержимое корневого каталога, а также дочерние узлы /zookeepe. 
 - Был создан узел /mynode с содержимым 'first_version', после чего произведена проверка его создания. 
 - Было получено значение этого узла и выведена метаинформация об узле.

![1](https://github.com/vasser2323/BigData/assets/73202398/1757f72c-f8c2-4070-a749-8bf80d9c7a72)

 - Далее было изменено содержимое узла /mynode и произведена проверка изменения.
 - Далее выведена обновленная метаинформация об узле.
   
![2](https://github.com/vasser2323/BigData/assets/73202398/856ee573-8b51-41a1-bb32-63dee636fd77)

 - Далее в узле /mynode были созданы дочерние sequentional-узлы

![3](https://github.com/vasser2323/BigData/assets/73202398/f4921ccb-46e9-4bf7-ab10-698eb73369d8)

 - Далее был создан узел /mygroup. Далее через новые консоли были созданы эфимерные узлы

 ![4](https://github.com/vasser2323/BigData/assets/73202398/796e7982-1c00-4443-adbb-36ba51c1e16d)

 - С основной консоли проведена проверка содержимого группы

![5](https://github.com/vasser2323/BigData/assets/73202398/fc39215b-8426-401b-8557-68774af86f9c)
![6](https://github.com/vasser2323/BigData/assets/73202398/6b21d139-b06f-41d7-8efd-b69ef2eb1a07)

- Далее из консоли grue получены данные узла bleen

![7](https://github.com/vasser2323/BigData/assets/73202398/2b6caff6-b5c2-4668-a2ab-535ed73ebaea)

 - Далее удалили узел bleen, дождались когда он удалится

![8](https://github.com/vasser2323/BigData/assets/73202398/e295fb97-4aee-44af-9e55-74609be7b9b7)

 - Далее создан узел /myconfig со значением 'sheep_count=1', проверка содержимого

![9](https://github.com/vasser2323/BigData/assets/73202398/45e20dcb-2827-45d7-9488-db592c24afc5)

 - Из другой консоли установлен watch-trigger на узел

![10](https://github.com/vasser2323/BigData/assets/73202398/cf34738d-411e-453f-ae64-ee341a4ab3df)
![11](https://github.com/vasser2323/BigData/assets/73202398/83522323-6ec3-4721-94fa-b3336fb48ff5)

- Далее удален узел /myconfig

![12](https://github.com/vasser2323/BigData/assets/73202398/8592f162-24ad-40d5-abc7-eedd5469d5e8)

## 2) Разработка распределенного приложения

2.1) До запуска любого из клиентов распределенного приложения узел /zoo пуст.

![13](https://github.com/vasser2323/BigData/assets/73202398/61859952-7c21-4268-be07-d633fa27374a)

2.2) После запуска первого эклемпляра программы в узел /zoo добавляется эфемерный узел monkey.

![14](https://github.com/vasser2323/BigData/assets/73202398/0559aec9-7327-471c-887e-25c703ec59e5)

2.3) После запуска второго эклемпляра программы в узел /zoo добавляется эфемерный узел "elephant". 
Первая программа при этом получает событие NodeChildredChanged и вычисляет количество зверей в зоопарке (2/3).

![15](https://github.com/vasser2323/BigData/assets/73202398/24eedb13-4262-4f55-98de-d7e377fedf35)

2.4) После запуска третьего и последнего эклемпляра программы в узел /zoo добавляется эфемерный узел "tiger". 
При этом все 3 клиента распределенного приложения получают событие NodeChildredChanged и определяют число зверей в зоопарке. 
Так как число достигло 3, все они начинают бежать.

![16](https://github.com/vasser2323/BigData/assets/73202398/84870e83-c426-4659-94e1-d6e2d4a599c4)

2.5) В конце все эфемерные узлы удаляются, и узел /zoo вновь становится пустым.

![17](https://github.com/vasser2323/BigData/assets/73202398/2a01de68-3b41-4575-89da-cd61b859a633)

## 3) Задача обедающх философов - за круглым столом сидят 5 философов,
каждый философ имеет перед собой тарелку с едой и вилки, которые лежат на столе между ним и его соседями. 
Философы по очереди берут две вилки, чтобы начать есть.

Выполнено классическое решение с помощью мьютексов:

Plato: Thinking

Thales: Thinking

Pythagoras: Thinking

Pythagoras: Thinking

Diogenes: Thinking

Thales: at the table

Thales: Picked up left fork

Plato: at the table

Plato: Picked up left fork

Thales : Picked up right fork

Pythagoras: at the table

Pythagoras: Picked up left fork

Diogenes: at the table

Diogenes: Picked up left fork

Thales- eating

Thales: Put down right fork

Thales: Put down left fork. Back to thinking

Plato: Picked up right fork

Thales: Thinking

Thales: at the table

Plato - eating

Plato: Put down right fork

Plato: Put down left fork. Back to thinking


Скрин выполнения:

![18](https://github.com/vasser2323/BigData/assets/73202398/79aa5692-bbfb-4614-918d-fcea2a574063)

## 4) Двухфазный коммит.

4.1) Фаза подготовки (prepare phase): координатор транзакций отправляет сообщения всем участникам, 
запрашивая подтверждение возможности выполнения транзакции. Каждый участник сохраняет 
состояние транзакции и отправляет ответ координатору (commit или abort).

4.2) Фаза коммита. Если все участники подтвердили транзакцию в фазе подготовки, то координатор отправляет 
commit-сообщения всем участникам, разрешая применени изменений. Каждый участник применяет изменения и отправляет 
подтверждающий ответ координатору.

Координатор регистрирует транзакционный узел /app/tx

Первая фаза: 

 - Координатор уведомляет исполнителей о транзакции
 - Координатор подписывается на изменения транзакционного узла (устанавливает WATCH на /app/tx)
 - Каждый исполнитель создает эфемерных узел /app/tx/node_i с решением commit/abort
 - Исполнитель подписывается на события своего узла для получения решения от координатора ( вторая фаза )

Вторая фаза:

 - Координатор принимает решение о commit/abort после ожидания таймаута или после создания всех узлов исполнителей с решением commit
 - Координатор изменяет значение эфемерных узлов для каждого исполнителя на commit / abort
 - Исполнители применяют / прерывают транзакцию
 - Исполнители обновляют значение узла на committed
