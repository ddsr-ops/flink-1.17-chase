package com.ddsr.sink;

class Staff {
        private final String name;
        private final int age;

        public Staff(String name, int age) {
            this.name = name;
            this.age = age;
        }

        @Override
        public String toString() {
            return "Staff{" + "name='" + name + '\'' + ", age=" + age + '}';
        }
    }