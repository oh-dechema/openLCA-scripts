from java.io import File
from org.openlca.core.database.derby import DerbyDatabase
from org.openlca.core.database import ProductSystemDao, ImpactMethodDao, ProcessDao, EntityCache
from org.openlca.core.math import CalculationSetup, SystemCalculator, CalculationType
from org.openlca.core.model.descriptors import Descriptors
from org.openlca.julia import JuliaSolver
from org.openlca.core.matrix.cache import MatrixCache
import org.openlca.core.model as model
import json 
from datetime import datetime
from collections import defaultdict



######################### Hier Werte anpassen!
# Pfad zur Datenbank (der Ordner!)
db_dir = File('C:/Users/oliver.hurtig/Documents/Software/openLCA/UBA Datenbank/databases/UBA_CCU')
# Pfad zu Ordner, in dem die Ergebnisse gespeichert werden
result_dir  = 'C:/Users/oliver.hurtig/Documents/Software/openLCA/UBA-results/'
# Trennzeichen für eindeutige Prozesspfade im Tree
sep = '//'
# Hier alle Namen der Produktsysteme eintragen, die berechnet werden
prod_systems = ['Syngas, Steam Methane Reforming', 'synthesis gas (2:1), mixing of CO and H2 from PEM', 'synthesis gas (2:1), mixing of CO and H2 from PYR']
# Globale Parameter: für jeden Wert in der Liste wird ein Szenario erstellt
strom_anteil_DE2017s = [0, 1]
anteil_DACs = [0, 1]
# Konvertiert die Parameterwerte in lesbare Namen (für jeden Parameterwert oben muss ein Name definiert werden!)
strom_name = ['EE2050', 'DE2017']
co2_name = ['AZ', 'DAC']

# Erstellt eine Liste mit Auswirkungen pro Kategorie. Der erste Teil ist der Name des Prozesses, der zweite Teil ist die Kategorie (für Grafiken)
categorize = {
  'Electrolyser, PEM': 'Anlage H2',
  'pyrolysis plant': 'Anlage H2',
  'DAC plant': 'Anlage CO2',
  'Amine scrubbing plant': 'Anlage CO2',
  'hydrogen, from electrolysis - EU-25': 'Bereitstellung H2',
  'hydrogen, from electrolysis': 'Bereitstellung H2',
  'hydrogen, from pyrolysis': 'Bereitstellung H2',
  'Carbon Dioxide, from direct air capture': 'Bereitstellung CO2',
  'Carbon Dioxide, from amine scrubbing': 'Bereitstellung CO2',
  'market for natural gas, low pressure | natural gas, low pressure | APOS, S - RoW': 'Bereitstellung fossile',
  'rWGS plant': 'Anlage Produkt'
}
#########################
db = DerbyDatabase(db_dir)
dao = ProductSystemDao(db)

def nested_dict():
  return defaultdict(nested_dict)
full_data = nested_dict()
units = {}

sums = {
  'Anlage H2': 0,
  'Anlage CO2': 0,
  'Anlage Produkt': 0,
  'Bereitstellung H2': 0,
  'Bereitstellung CO2': 0,
  'Bereitstellung fossile': 0,
  'Bereitstellung Produkt': 0
}

for prod_system in prod_systems:
  for strom_anteil_DE2017 in strom_anteil_DE2017s:
    for anteil_DAC in anteil_DACs:
      system = dao.getForName(prod_system)[0]
      solver = JuliaSolver()
      m_cache = MatrixCache.createLazy(db)
      calculator = SystemCalculator(m_cache, solver)
      setup = CalculationSetup(CalculationType.UPSTREAM_ANALYSIS, system)

      ### Parameter für Berechnung festlegen
      strom = model.ParameterRedef()
      strom.value = strom_anteil_DE2017
      strom.name = 'strom_anteil_DE2017'
      setup.parameterRedefs.add(strom)

      capture = model.ParameterRedef()
      capture.value = anteil_DAC
      capture.name = 'anteil_DAC'
      setup.parameterRedefs.add(capture)

      ### Die Wirkungsabschätzungsmethode auswählen
      method_dao = ImpactMethodDao(db)
      impactMethod = method_dao.getForName('Z:UBA')[0]
      setup.impactMethod = Descriptors.toDescriptor(impactMethod)

      result = calculator.calculateFull(setup)
      
      data = {
        'processContrib': {},
        'categoryContrib': {}
      }

      ### Der "tree" beinhaltet alle Ergebnisse aus dem Blatt "Contribution Tree" in openLCA
      def traverseTree(tree, path, node, cat_name, is_Root=True):
        children = tree.childs(node)
        # Ein eindeutiger Pfad, der den Prozess in der Kette definiert
        new_path = path + sep + node.provider.process.name
        if new_path not in data['processContrib']:
          data['processContrib'][new_path] = {}
        # Ergebnis des Prozesses speichern
        data['processContrib'][new_path][cat_name] = node.result
        # Ergebnis der Kategorie hinzufügen
        if node.provider.process.name in categorize:
          data['categoryContrib'][cat_name][categorize[node.provider.process.name]] += node.result
        if is_Root:
          data['categoryContrib'][cat_name]['Bereitstellung Produkt'] = node.result
        # Rekursiv weitermachen
        for child in children:
          traverseTree(tree, new_path, child, cat_name, is_Root=False)

      for cat in impactMethod.impactCategories:
        tree = result.getTree(Descriptors.toDescriptor(cat))
        cat_name = cat.name
        units[cat_name] = tree.ref.referenceUnit
        data['categoryContrib'][cat_name] = sums.copy()
        traverseTree(tree, "", tree.root, cat_name)
        ### Bereitstellung Produkt beinhaltet alles, also wieder abziehen
        data['categoryContrib'][cat_name]['Bereitstellung Produkt'] = data['categoryContrib'][cat_name]['Bereitstellung Produkt'] - data['categoryContrib'][cat_name]['Anlage Produkt'] - data['categoryContrib'][cat_name]['Bereitstellung H2'] - data['categoryContrib'][cat_name]['Bereitstellung CO2'] - data['categoryContrib'][cat_name]['Bereitstellung fossile']
        ### Bereitstellung beinhaltet Anlagen, also wieder abziehen
        data['categoryContrib'][cat_name]['Bereitstellung H2'] = data['categoryContrib'][cat_name]['Bereitstellung H2']- data['categoryContrib'][cat_name]['Anlage H2']
        data['categoryContrib'][cat_name]['Bereitstellung CO2'] = data['categoryContrib'][cat_name]['Bereitstellung CO2'] - data['categoryContrib'][cat_name]['Anlage CO2']
      
      full_data[prod_system][co2_name[anteil_DAC]][strom_name[strom_anteil_DE2017]] = data

now = datetime.now()
FILE = result_dir + now.strftime("%Y-%m-%d %H.%M") + '.json'
with open(FILE, 'w') as f:
  f.write(json.dumps(full_data))

FILE = result_dir + now.strftime("%Y-%m-%d %H.%M") + '_units.json'
with open(FILE, 'w') as f:
  f.write(json.dumps(units))