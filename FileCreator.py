def process_line(line):
    parts = line.split('\t')
    if len(parts) < 3:
        return None, None
    effect, words = parts[1], parts[2]
    words_list = words.split(',')
    return effect.strip(), [word.strip() for word in words_list]

def main(file_path):
    positive_words = []
    negative_words = []
    neutral_words = []

    with open("EffectWordNet.tff", 'r') as file:
        for line in file:
            effect, words = process_line(line)
            if effect == '+Effect':
                positive_words.extend(words)
            elif effect == '-Effect':
                negative_words.extend(words)
            elif effect == 'Null':
                neutral_words.extend(words)

    with open('positive_words.txt', 'w') as pos_file:
        pos_file.write('\n'.join(positive_words))

    with open('negative_words.txt', 'w') as neg_file:
        neg_file.write('\n'.join(negative_words))

    with open('neutral_words.txt', 'w') as neu_file:
        neu_file.write('\n'.join(neutral_words))

if __name__ == '__main__':
    file_path = 'path_to_your_EffectWordnet_file.tff'  # Update with your file path
    main(file_path)
