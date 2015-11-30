# this program will run in the background and query ers for entities that match prop/val or prop and cache them

from cli_interface import *
import time

def main():
    interface = Interface()
    while True:
        for prop in properties:
            entities_list = interface.search_for_entity(prop)
            for entity_id in entities_list:
                entity = interface.ers.get(entity_id)
                interface.ers.cache_entity(entity)
        for prop in prop_val:
            entities_list = interface.search_for_entity(prop, prop_val[prop])
            for entity_id in entities_list:
                entity = interface.ers.get(entity_id)
                interface.ers.cache_entity(entity)

    time.sleep(1)

if __name__ == "__main__":
    main()
