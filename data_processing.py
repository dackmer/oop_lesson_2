import csv, os

__location__ = os.path.realpath(
    os.path.join(os.getcwd(), os.path.dirname(__file__)))

cities = []
with open(os.path.join(__location__, 'Cities.csv')) as f:
    rows = csv.DictReader(f)
    for r in rows:
        cities.append(dict(r))

countries = []
with open(os.path.join(__location__, 'Countries.csv')) as f:
    rows = csv.DictReader(f)
    for r in rows:
        countries.append(dict(r))

titanic = []
with open(os.path.join(__location__, 'Titanic.csv')) as f:
    rows = csv.DictReader(f)
    for r in rows:
        titanic.append(dict(r))

players = []
with open(os.path.join(__location__, 'Players.csv')) as f:
    rows = csv.DictReader(f)
    for r in rows:
        players.append(dict(r))

teams = []
with open(os.path.join(__location__, 'Teams.csv')) as f:
    rows = csv.DictReader(f)
    for r in rows:
        teams.append(dict(r))

class DB:
    def __init__(self):
        self.database = []

    def insert(self, table):
        self.database.append(table)

    def search(self, table_name):
        for table in self.database:
            if table.table_name == table_name:
                return table
        return None
    
import copy
class Table:
    def __init__(self, table_name, table):
        self.table_name = table_name
        self.table = table
    
    def join(self, other_table, common_key):
        joined_table = Table(self.table_name + '_joins_' + other_table.table_name, [])
        for item1 in self.table:
            for item2 in other_table.table:
                if item1[common_key] == item2[common_key]:
                    dict1 = copy.deepcopy(item1)
                    dict2 = copy.deepcopy(item2)
                    dict1.update(dict2)
                    joined_table.table.append(dict1)
        return joined_table
    
    def filter(self, condition):
        filtered_table = Table(self.table_name + '_filtered', [])
        for item1 in self.table:
            if condition(item1):
                filtered_table.table.append(item1)
        return filtered_table

    def __is_float(self, element):
        if element is None:
            return False
        try:
            float(element)
            return True
        except ValueError:
            return False

    def aggregate(self, function, aggregation_key):
        temps = []
        for item1 in self.table:
            if self.__is_float(item1[aggregation_key]):
                temps.append(float(item1[aggregation_key]))
            else:
                temps.append(item1[aggregation_key])
        return function(temps)

    def select(self, attributes_list):
        temps = []
        for item1 in self.table:
            dict_temp = {}
            for key in item1:
                if key in attributes_list:
                    dict_temp[key] = item1[key]
            temps.append(dict_temp)
        return temps

    def pivot_table(self, keys_to_pivot_list, keys_to_aggreagte_list, aggregate_func_list):
        # First create a list of unique values for each key
        unique_values_list = []

        # Here is an example of of unique_values_list for
        # keys_to_pivot_list = ['embarked', 'gender', 'class']
        # unique_values_list =
        # [['Southampton', 'Cherbourg', 'Queenstown'], ['M', 'F'], ['3', '2',
        # '1']]

        # Get the combination of unique_values_list
        # You will make use of the function you implemented in Task 2

        import combination_gen

        # code that makes a call to combination_gen.gen_comb_list

        # Example output:
        # [['Southampton', 'M', '3'],
        #  ['Cherbourg', 'M', '3'],
        #  ...
        #  ['Queenstown', 'F', '1']]

        # code that filters each combination

        # for each filter table applies the relevant aggregate functions
        # to keys to aggregate
        # the aggregate functions is listed in aggregate_func_list
        # to keys to aggregate is listed in keys_to_aggreagte_list

        # return a pivot table

        unique_values_list = []
        for key in keys_to_pivot_list:
            unique_values = list(set(item[key] for item in self.table))
            unique_values_list.append(unique_values)
        print(unique_values_list)
        import combination_gen
        combinations = combination_gen.gen_comb_list(unique_values_list)
        print(combinations)
        pivot_table_data = []
        for combination in combinations:
            filter_table = self
            for i, key in enumerate(keys_to_pivot_list):
                filter_table = filter_table.filter(lambda x: x[key] == combination[i])
            pivot_key = tuple(combination)
            pivot_table_data = []
            for i, key in enumerate(keys_to_aggreagte_list):
                for func in aggregate_func_list:
                    if key in pivot_table_data:
                        pivot_table_data.append(filter_table.select([key]))
                    else:
                        pivot_table_data.append(filter_table.select([key]))

        return pivot_table_data

    def __str__(self):
        return self.table_name + ':' + str(self.table)

table1 = Table('cities', cities)
table2 = Table('countries', countries)
table3 = Table('titanic', titanic)
table4 = Table('teams', teams)
table5 = Table('players', players)

my_DB = DB()
my_DB.insert(table1)
my_DB.insert(table2)
my_DB.insert(table3)
my_DB.insert(table4)
my_DB.insert(table5)
my_table1 = my_DB.search('cities')

print("Test filter: only filtering out cities in Italy") 
my_table1_filtered = my_table1.filter(lambda x: x['country'] == 'Italy')
print(my_table1_filtered)
print()

print("Test select: only displaying two fields, city and latitude, for cities in Italy")
my_table1_selected = my_table1_filtered.select(['city', 'latitude'])
print(my_table1_selected)
print()

print("Calculting the average temperature without using aggregate for cities in Italy")
temps = []
for item in my_table1_filtered.table:
    temps.append(float(item['temperature']))
print(sum(temps)/len(temps))
print()

print("Calculting the average temperature using aggregate for cities in Italy")
print(my_table1_filtered.aggregate(lambda x: sum(x)/len(x), 'temperature'))
print()

print("Test join: finding cities in non-EU countries whose temperatures are below 5.0")
my_table2 = my_DB.search('countries')
my_table3 = my_table1.join(my_table2, 'country')
my_table3_filtered = my_table3.filter(lambda x: x['EU'] == 'no').filter(lambda x: float(x['temperature']) < 5.0)
print(my_table3_filtered.table)
print()
print("Selecting just three fields, city, country, and temperature")
print(my_table3_filtered.select(['city', 'country', 'temperature']))
print()

print("Print the min and max temperatures for cities in EU that do not have coastlines")
my_table3_filtered = my_table3.filter(lambda x: x['EU'] == 'yes').filter(lambda x: x['coastline'] == 'no')
print("Min temp:", my_table3_filtered.aggregate(lambda x: min(x), 'temperature'))
print("Max temp:", my_table3_filtered.aggregate(lambda x: max(x), 'temperature'))
print()

print("Print the min and max latitude for cities in every country")
for item in my_table2.table:
    my_table1_filtered = my_table1.filter(lambda x: x['country'] == item['country'])
    if len(my_table1_filtered.table) >= 1:
        print(item['country'], my_table1_filtered.aggregate(lambda x: min(x), 'latitude'), my_table1_filtered.aggregate(lambda x: max(x), 'latitude'))
print()


def find_players():
    filtered_players = []
    for player in players:
        try:
            minutes_played = int(player['minutes_played'])
        except KeyError:
            continue

        if minutes_played < 200 and int(player['passes']) > 100:
            filtered_players.append(player)

    selected_players = []
    for player in filtered_players:
        selected_player = {
            'surname': player['surname'],
            'team': player['team'],
            'position': player['position']
        }
        selected_players.append(selected_player)

    return selected_players


selected_players = find_players()

print("The average number of games played for teams ranking below 10 versus")
below_rank_10 = table4.filter(lambda x: int(x['ranking']) < 10)
avg_games_below_rank_10 = below_rank_10.aggregate(lambda x: sum(x) / len(x), 'games')
print("Average games played for teams below rank 10:", avg_games_below_rank_10)
print("The average number of games played for teams ranking above or equal 10")
rank_10_or_above = table4.filter(lambda x: int(x['ranking']) >= 10)
avg_games_rank_10_or_above = rank_10_or_above.aggregate(lambda x: sum(x) / len(x), 'games')
print("Average games played for teams rank 10 or above:", avg_games_rank_10_or_above)
print()

print("The average number of passes made by forwards versus by midfielders")
forwards = table5.filter(lambda x: x['position'] == 'forward')
avg_passes_forwards = forwards.aggregate(lambda x: sum(x) / len(x), 'passes')
print("Average passes by forwards:", avg_passes_forwards)
midfielders = table5.filter(lambda x: x['position'] == 'midfielder')
avg_passes_midfielders = midfielders.aggregate(lambda x: sum(x) / len(x), 'passes')
print("Average passes by midfielders:", avg_passes_midfielders)
print()

print("The average fare paid by passengers in the first class versus in the third class")
titanic_table = Table('titanic', titanic)
first_class_passengers = table3.filter(lambda x: x['class'] == '1')
avg_fare_first_class = first_class_passengers.aggregate(lambda x: sum(x) / len(x), 'fare')
print("Average fare for first class passengers:", avg_fare_first_class)
third_class_passengers = table3.filter(lambda x: x['class'] == '3')
avg_fare_third_class = third_class_passengers.aggregate(lambda x: sum(x) / len(x), 'fare')
print("Average fare for third class passengers:", avg_fare_third_class)
print()


print("The survival rate of male versus female passengers")
male_passengers = table3.filter(lambda x: x['gender'] == 'M')
male_survived = [x for x in male_passengers.table if x['survived'] == 'yes']
male_survival_rate = len(male_survived) / len(male_passengers.table)
print("Survival rate for male passengers:", male_survival_rate)

female_passengers = table3.filter(lambda x: x['gender'] == 'F')
female_survived = [x for x in female_passengers.table if x['survived'] == 'yes']
female_survival_rate = len(female_survived) / len(female_passengers.table)
print("Survival rate for female passengers:", female_survival_rate)

print()

print("Find the total number of male passengers embarked at Southampton")
male_passengers_southampton = table3.filter(lambda x: x['gender'] == 'M' and x['embarked'] == 'Southampton')
total_male_passengers_southampton = len(male_passengers_southampton.table)
print("Total number of male passengers embarked at Southampton:", total_male_passengers_southampton)


table4 = Table('titanic', titanic)
my_DB.insert(table4)
my_table4 = my_DB.search('titanic')
my_pivot = my_table4.pivot_table(['embarked', 'gender', 'class'],
                                 ['fare', 'fare', 'fare', 'last'],
                                 [lambda x: min(x), lambda x: max(x),
                                  lambda x: sum(x)/len(x), lambda x: len(x)])


