package com.mobius.dt.infrastructure.onem2m.provision;

import java.util.List;
import java.util.Map;

public class Onem2mProvisionPlan {

    public AeSpec ae;
    public List<CntSpec> tree;

    public static class AeSpec {

        public String rn;
        public String api;
        public Boolean rr;
        public List<String> poa;
    }

    public static class CntSpec {

        public String rn;
        public List<String> lbl;
        public Integer mni;
        public Integer mia;
        public List<CntSpec> cnt;
        public List<SubSpec> subs;
    }

    public static class SubSpec {

        public String rn;
        public Map<String,Object> enc;
        public List<String> nu;
        public Integer nct;
    }
}
