def __find_key_recursively(data: dict | list, target_key: str):
    """Função auxiliar com recursividade para encontrar a chave especificada.

    Args:
        data (dict): Dicionário onde o valor da chave está presente.
        target_key (str): Chave a ser encontrada.

    Returns:
        Union[str, dict, list]:
            Retorna o valor da chave encontrada, podendo ser uma string, um dict ou um list.
    """
    #Caso o valor de data seja uma lista, faz uma iteração sobre todos os elementos da lista passando o elemento
    if isinstance(data, list):
        for element in data:
            result = __find_key_recursively(element, target_key)
            if result != "0":
                return result
            
    for key, value in data.items():
        #Condição de parada principal
        if key == target_key:
            return value
        #Chama a função novamente passando como parâmetro o value da iteração caso ele seja um dict.
        elif isinstance(value, dict):
            result = __find_key_recursively(value, target_key)
            if result != "0":
                return result
        #Caso o value seja do tipo list, faz uma iteração sobre todos os itens da lista passando o elemento
        # de cada iteração como argumento da função recursiva.
        elif isinstance(value, list):
            for element in value:
                result = __find_key_recursively(element, target_key)
                if result != "0":
                    return result
    return "0" 


def find_key(data: dict, target_key: str):
    """
    Encontra uma chave em um dicionario utilizando um método auxiliar de recursividade. Caso a chave-alvo seja repetida no dicionario,
    colocar o valor de seu parente mais próximo separando por espaço.

    Ex.: dict = {
        "milhas" : {
            "altura" = "x",
            "largura" = "y"
        }
        "kilometros" : {
            "altura" = "2x",
            "largura" = "2y"
        }
    }

    find_key(dict, "kilometros largura") -> "2y"

    Args:
        data (dict): Dicionário onde o valor da chave está presente.
        target_key (str): Chave a ser encontrada.

    Returns:
        Union[str, dict, list]:
            Retorna o valor da chave encontrada, podendo ser uma string, um dict ou um list.
    """
    
    if len(target_key.split(" ")) > 1:
        keys_steps = target_key.split(" ")
        new_data = data

        for i in range(len(keys_steps)):
            new_data = __find_key_recursively(new_data, keys_steps[i])
        return new_data if new_data != "0" else "Chave não encontrada."
    
    else:
        result =  __find_key_recursively(data, target_key=target_key)
        return result if result != "0" else "Chave não encontrada."


if __name__ == "__main__":

    dictionary = {
        "milhas" : {
            "altura" : "x",
            "largura" : "y"
        },
        "kilometros" : {
            "altura" : "2x",
            "largura" : "2y"
        }
    }

    try:
        print(find_key(dictionary, "kilometros largura"))
    except Exception as e:
        print(e)



