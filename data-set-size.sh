echo -n \'DATA=data/urls-short KEY_COUNT=$(echo $2/\(62.204+$1\) | bc)\'' '
echo -n \'DATA=data/urls KEY_COUNT=$(echo $2/\(62.280+$1\) | bc)\'' '
echo -n \'DATA=data/wiki KEY_COUNT=$(echo $2/\(22.555+$1\) | bc)\'' '
echo -n \'DATA=int KEY_COUNT=$(echo $2/\(4+$1\) | bc)\'' '
echo -n \'DATA=rng4 KEY_COUNT=$(echo $2/\(4+$1\) | bc)\'' '
echo -n \'DATA=rng8 KEY_COUNT=$(echo $2/\(8+$1\) | bc)\'' '
echo
