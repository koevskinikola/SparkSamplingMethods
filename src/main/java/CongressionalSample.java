import org.apache.commons.beanutils.converters.IntegerArrayConverter;
import org.apache.commons.collections.map.HashedMap;
import scala.Tuple2;
import twitter4j.Status;
import twitter4j.TwitterStream;
import twitter4j.auth.Authorization;

import java.util.*;

/**
 * Created by nikola on 5.5.16.
 */
public abstract class CongressionalSample<K, V> {
    Integer sampleSize;
    Integer count;
    Integer totalMax;
    Random random;

    //Define categories
    final List<V> groupList;
    final Map<V, HashMap<V, Integer>> groupingsMap;
    final Map<List<V>, Integer> groupsMap;
    final Map<Tuple2<V, V>, Float> sampleSizeMap;

    final List<K> uniformReservoir;
    final Map<List<V>, List<K>> uniformReservoirGrouped;
    final Map<V, HashMap<V, Tuple2<ArrayList<Tuple2<K, List<V>>>, Integer>>> groupingReservoirs;
    final Map<List<V>, Tuple2<ArrayList<K>, Integer>> senateReservoirs;
    final Map<List<K>, Integer> congressSample;

    List<K> sample;

    public CongressionalSample(Integer sampleSize, List<V> groupList) {
        this.sampleSize = sampleSize;
        this.groupList = groupList;

        count = 0;
        random = new Random();
        groupingsMap = new HashMap<>();
        groupsMap = new HashMap<>();
        sampleSizeMap = new HashMap<>();

        uniformReservoir = new ArrayList<>(sampleSize);
        uniformReservoirGrouped = new HashMap<>();
        groupingReservoirs = new HashMap<>();
        senateReservoirs = new HashMap<>();

        for (V group : groupList) {
            groupingsMap.put(group, new HashMap<V, Integer>());
            groupingReservoirs.put(group, new HashMap<V, Tuple2<ArrayList<Tuple2<K, List<V>>>, Integer>>());
        }

        congressSample = new HashMap<>();
    }

    public void singleSample(K item) {
        uniformSample(item);
        senateSample(item);
        countTuples(item);
        groupingSample(item);

        count++;
    }

    public void listSample(List<K> items) {
        for (K item:items) {
            singleSample(item);
        }
    }

    public void uniformSample(K item) {
        if (count < sampleSize) {
            uniformReservoir.add(item);
        } else {
            int randomPos = random.nextInt(count);
            if (randomPos < sampleSize) {
                uniformReservoir.add(randomPos, item);
            }
        }
    }

    public void senateSample(K item) {
        int senateSlotSize = sampleSize / senateReservoirs.size();
        List<V> key = getGroupList(item);
        if (senateReservoirs.containsKey(key)) {
            Tuple2<ArrayList<K>, Integer> reservoir = senateReservoirs.get(key);
            ArrayList<K> tmpList = reservoir._1();
            int slotCount = reservoir._2();

            if (slotCount < senateSlotSize) {
                tmpList.add(item);
            } else {
                int randomPos = random.nextInt(count);
                if (randomPos < senateSlotSize) {
                    tmpList.add(randomPos, item);
                }
            }
            slotCount++;
            senateReservoirs.put(key, new Tuple2<ArrayList<K>, Integer>(tmpList, slotCount));
        } else {
            ArrayList<K> tmpList = new ArrayList<>();
            tmpList.add(item);
            senateReservoirs.put(key, new Tuple2<ArrayList<K>, Integer>(tmpList, 1));
        }
    }

    public void groupingSample(K item) {
        List<V> tmpList = getGroupList(item);
        for (V grouping:groupList) {
            int groupingCount = groupingsMap.get(grouping).size();
            int grouping1SlotSize = sampleSize / groupingCount;
            V key = get(item, grouping);
            HashMap<V, Tuple2<ArrayList<Tuple2<K, List<V>>>, Integer>> groupingReservoir = groupingReservoirs.get(grouping);

            if (groupingReservoir.containsKey(key)) {
                Tuple2<ArrayList<Tuple2<K, List<V>>>, Integer> tmpReservoir = groupingReservoir.get(key);
                int groupCount = tmpReservoir._2();
                if (groupCount < grouping1SlotSize) {
                    tmpReservoir._1().add(new Tuple2<K, List<V>>(item, tmpList));
                } else {
                    int randomPos = random.nextInt(groupCount);
                    if (randomPos < grouping1SlotSize) {
                        tmpReservoir._1().add(randomPos, new Tuple2<K, List<V>>(item, tmpList));
                    }
                }
                groupCount++;
                tmpReservoir = tmpReservoir.copy(tmpReservoir._1, groupCount);
                groupingReservoir.put(key, tmpReservoir);
            } else {
                int groupCount = 0;
                ArrayList<Tuple2<K, List<V>>> groupList = new ArrayList<>();
                if (groupCount < grouping1SlotSize) {
                    groupList.add(new Tuple2<K, List<V>>(item, tmpList));
                }
                groupCount++;
                Tuple2<ArrayList<Tuple2<K, List<V>>>, Integer> tmpReservoir = new Tuple2<>(groupList, groupCount);
                groupingReservoir.put(key, tmpReservoir);
            }
            groupingReservoirs.put(grouping, groupingReservoir);
        }
    }


    //TODO Not needed?
    public void countTuples(K item) {
        List<V> tmpList = new ArrayList<>(groupList.size());
        int index = 0;
        for (V grouping:groupList) {
            HashMap<V, Integer> groupingMap = groupingsMap.get(grouping);
            V key = get(item, grouping);
            if (groupingMap.containsKey(key)) {
                int groupingCount = groupingMap.get(key);
                groupingMap.put(key, groupingCount + 1);

            } else {
                groupingMap.put(key, 1);
            }
            tmpList.add(index, key);
            index++;
        }

        if (groupsMap.containsKey(tmpList))
            groupsMap.put(tmpList, groupsMap.get(tmpList) + 1);
        else
            groupsMap.put(tmpList, 1);
    }

    public void groupUniformReservoir() {
        for (K item:uniformReservoir) {
            List<V> key = getGroupList(item);

            if (uniformReservoirGrouped.containsKey(key)) {
                List<K> tmpList = uniformReservoirGrouped.get(key);
                tmpList.add(item);
                uniformReservoirGrouped.put(key, tmpList);
            } else {
                List<K> tmpList = new ArrayList<>();
                tmpList.add(item);
                uniformReservoirGrouped.put(key, tmpList);
            }
        }
    }

    public void calculateSampleSlotSize() {
        int totalMax = 0;
        for (Map.Entry group:groupsMap.entrySet()) {

            //Get attribute values (groups)
            List<V> groupings = (List<V>)group.getKey();
            int index = 0;

            List<K> maxList = new ArrayList<>();
            int maxSize = 0;

            for (V grouping:groupings) {

                //the Group-by attribute/column
                V groupingLabel = groupList.get(index);

                //The number of values per attribute (mT)
                int gCount = groupingReservoirs.get(groupingLabel).size();

                //the Attribute reservoir
                ArrayList<Tuple2<K, List<V>>> reservoir = groupingReservoirs.get(groupingLabel).get(grouping)._1();

                //the Attribute tuple count (nH)
                int groupingCount = reservoir.size();

                //the per-group Attribute tuple count (nG)
                int groupCount = 0;

                List<K> tmpList = new ArrayList<>();
                for (Tuple2<K, List<V>> tuple:reservoir) {
                    if (tuple._2().equals(groupings)) {
                        tmpList.add(tuple._1());
                        groupCount++;
                    }
                }

                int estimetedSampleSize = (sampleSize * groupCount) / (gCount * groupingCount);
                if (estimetedSampleSize > maxSize) {
                    maxSize = estimetedSampleSize;
                    maxList = tmpList;
                }
                index++;
            }

            //Compare to uniform group
            int uniformSize = uniformReservoirGrouped.get(groupings).size();
            if (maxSize < uniformSize) {
                maxSize = uniformSize;
                maxList = uniformReservoirGrouped.get(groupings);
            }

            //Compare to Senate group
            int senateSize = senateReservoirs.get(groupings)._1().size();
            if (maxSize < senateSize) {
                maxSize = senateSize;
                maxList = senateReservoirs.get(groupings)._1();
            }

            totalMax += maxSize;
            this.totalMax = totalMax;
            congressSample.put(maxList, maxSize);
        }
    }

    public void scaleDownSample() {
        for (Map.Entry groupSample:congressSample.entrySet()) {
            List<K> sampleList = (List<K>)groupSample.getKey();
            int maxSize = (int)groupSample.getValue();
            int slotSampleSize = (this.sampleSize * maxSize) / this.totalMax;
            int counter = 0;
            for (K item:sampleList) {
                if (counter < slotSampleSize) {
                    sample.add(item);
                } else {
                    int randomPos = random.nextInt(counter);
                    if (randomPos < slotSampleSize) {
                        sample.add(randomPos, item);
                    }
                }
            }
        }
    }

    public List<V> getGroupList(K item) {
        List<V> tmpList = new ArrayList<>(groupList.size());
        int index = 0;
        for (V grouping:groupList) {
            V key = get(item, grouping);
            tmpList.add(index, key);
            index++;
        }

        return tmpList;
    }

    public Integer getCount() {
        return count;
    }

    public List<K> getSample() {
        calculateSampleSlotSize();
        scaleDownSample();

        return sample;
    }

    /**
     * Define general getter function for all possible groups
     *
     * @param item Object that contains groups' instances
     * @param group The selected instance's group
     * @return Return the group instance. Has to be the same type as the group's type
     */
    public abstract V get(K item, V group);
}
