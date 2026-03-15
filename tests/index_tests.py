from src.infrastructure.db.falkordb_client import falkor_client


def check_indexes_procedure():
    graph = falkor_client.get_graph("movies_knowledge_graph")

    print("Проверка db.indexes")
    try:
        result = graph.query("CALL db.indexes()")

        if not result.result_set:
            print("существует но индексов нет")
        else:
            print("Найденные индексы")
            for row in result.result_set:
                print(f"  - {row}")

    except Exception as e:
        print(f"{e}")


if __name__ == "__main__":
    check_indexes_procedure()