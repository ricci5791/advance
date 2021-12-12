mkdir hw1 hw1/data hw1/bin

cp "$0" hw1/bin
chmod 777 hw1/bin/script.sh

FILE_COUNT=1

WROTE_NUMBER=5
TOTAL_NUMBERS=$1

while [ $TOTAL_NUMBERS -gt 0 ]; do
  if [ "$WROTE_NUMBER" -gt 0 ]; then
    RANDOM_NUMBER=$( printf "%d.%d\n" $RANDOM $RANDOM )
    echo "$RANDOM_NUMBER" >> "hw1/data/rnd$FILE_COUNT.txt"
    WROTE_NUMBER=$(($WROTE_NUMBER - 1))
    TOTAL_NUMBERS=$(($TOTAL_NUMBERS - 1))
  fi

  if [ "$WROTE_NUMBER" -eq 0 ]; then
    WROTE_NUMBER=5
    FILE_COUNT=$(($FILE_COUNT + 1))
  fi

done