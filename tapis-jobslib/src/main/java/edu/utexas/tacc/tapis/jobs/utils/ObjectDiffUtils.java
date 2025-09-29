package edu.utexas.tacc.tapis.jobs.utils;

import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import java.lang.reflect.Type;
import java.util.Map;
import java.util.List;
import java.util.Set;
import java.util.HashMap;
import java.util.stream.Collectors;

public class ObjectDiffUtils {

    public static class ListDiff<T> {
        private Map<T, Integer> addedElements;
        private Map<T, Integer> removedElements;

        public Map<T, Integer> getAddedElements() {
            return addedElements;
        }
        public void setAddedElements(Map<T, Integer> addedElements) {
            this.addedElements = addedElements; 
        }

        public Map<T, Integer> getRemovedElements() {
            return removedElements;
        }

        public void setRemovedElements(Map<T, Integer> removedElements) {
            this.removedElements = removedElements;
        }

        public String toJsonString () {
            Gson gson = new Gson();
            return gson.toJson(this);
        }
    }

    public static class SetDiff<T> {
        private Set<T> addedElements;
        private Set<T> removedElements;

        public Set<T> getAddedElements() {
            return addedElements;
        }
        public void setAddedElements(Set<T> addedElements) {
            this.addedElements = addedElements; 
        }

        public Set<T> getRemovedElements() {
            return removedElements;
        }

        public void setRemovedElements(Set<T> removedElements) {
            this.removedElements = removedElements;
        }

        public String toJsonString () {
            Gson gson = new Gson();
            return gson.toJson(this);
        }
    }

    public static class ObjectDiff {
        private Map<String, Object> addedFields;
        private Map<String, Object> removedFields;
        private Map<String, MapDifference.ValueDifference<Object>> modifiedFields;

        public Map<String, Object> getAddedFields() {
            return addedFields;
        }

        public void setAddedFields(Map<String, Object> addedFields) {
            this.addedFields = addedFields;
        }

        public Map<String, Object> getRemovedFields() {
            return removedFields;
        }

        public void setRemovedFields(Map<String, Object> removedFields) {
            this.removedFields = removedFields;
        }

        public Map<String, MapDifference.ValueDifference<Object>> getModifiedFields() {
            return modifiedFields;
        }

        public void setModifiedFields(Map<String, MapDifference.ValueDifference<Object>> modifiedFields) {
            this.modifiedFields = modifiedFields;
        }

        public String toJsonString () {
            Gson gson = new Gson();
            return gson.toJson(this);
        }
    }

    

    public static ObjectDiff computeObjectDiff(Object oldObject, Object newObject) {
        Gson gson = new Gson();
        
        // Use TypeToken to handle generic map types
        Type mapType = new TypeToken<Map<String, Object>>() {}.getType();

        // Convert Java objects to maps
        Map<String, Object> oldMap = gson.fromJson(gson.toJson(oldObject), mapType);
        Map<String, Object> newMap = gson.fromJson(gson.toJson(newObject), mapType);

        // Find the differences
        MapDifference<String, Object> difference = Maps.difference(oldMap, newMap);
        ObjectDiff objDiff = new ObjectDiff();
        objDiff.setAddedFields(difference.entriesOnlyOnRight());
        objDiff.setRemovedFields(difference.entriesOnlyOnLeft());
        objDiff.setModifiedFields(difference.entriesDiffering());
        return objDiff;
    }

    public static <T> SetDiff<T> computeSetDiff(Set<T> oldSet, Set<T> newSet) {
        SetDiff<T> arrayDiff = new SetDiff<>();
        // Find removed elements (in old but not in new)
        Set<T> removed = oldSet.stream()
                .filter(element -> !newSet.contains(element))
                .collect(Collectors.toSet());

        // Find added elements (in new but not in old)
        Set<T> added = newSet.stream()
                .filter(element -> !oldSet.contains(element))
                .collect(Collectors.toSet());

        arrayDiff.setAddedElements(added);
        arrayDiff.setRemovedElements(removed);
        return arrayDiff;
    }

    public static <T> ListDiff<T> computeListDiff(List<T> oldList, List<T> newList) {
        ListDiff<T> listDiff = new ListDiff<>();

        // Count frequencies in old and new lists
        Map<T, Integer> oldFreq = new HashMap<>();
        Map<T, Integer> newFreq = new HashMap<>();

        for (T item : oldList) {
            oldFreq.put(item, oldFreq.getOrDefault(item, 0) + 1);
        }

        for (T item : newList) {
            newFreq.put(item, newFreq.getOrDefault(item, 0) + 1);
        }

        // Find added elements (in new but not in old, or more in new than old)
        Map<T, Integer> added = new HashMap<>();
        for (Map.Entry<T, Integer> entry : newFreq.entrySet()) {
            T element = entry.getKey();
            int newCount = entry.getValue();
            int oldCount = oldFreq.getOrDefault(element, 0);
            if (newCount > oldCount) {
                added.put(element, newCount - oldCount);
            }
        }

        // Find removed elements (in old but not in new, or more in old than new)
        Map<T, Integer> removed = new HashMap<>();
        for (Map.Entry<T, Integer> entry : oldFreq.entrySet()) {
            T element = entry.getKey();
            int oldCount = entry.getValue();
            int newCount = newFreq.getOrDefault(element, 0);
            if (oldCount > newCount) {
                removed.put(element, oldCount - newCount);
            }
        }

        listDiff.setAddedElements(added);
        listDiff.setRemovedElements(removed);
        return listDiff;
    }
}