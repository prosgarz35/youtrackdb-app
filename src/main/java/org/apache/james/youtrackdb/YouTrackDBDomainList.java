package org.apache.james.youtrackdb;

import java.util.ArrayList;
import java.util.List;

import jakarta.inject.Inject;

import org.apache.james.core.Domain;
import org.apache.james.dnsservice.api.DNSService;
import org.apache.james.domainlist.api.DomainListException;
import org.apache.james.domainlist.lib.AbstractDomainList;

import com.jetbrains.youtrackdb.api.gremlin.YTDBGraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Vertex;

public class YouTrackDBDomainList extends AbstractDomainList {
    private static final String CLASS_NAME = "JamesDomain";
    private static final String PROP_DOMAIN = "domain";

    private final YTDBGraphTraversalSource g;

    @Inject
    public YouTrackDBDomainList(DNSService dnsService, YTDBGraphTraversalSource g) {
        super(dnsService);
        this.g = g;
    }

    @Override
    public void addDomain(Domain domain) throws DomainListException {
        try {
            YouTrackDBTransactions.executeStrictTx(g, tx -> {
                boolean exists = false;
                try {
                    exists = !tx.yql("SELECT 1 FROM JamesDomain WHERE domain = :domain LIMIT 1", "domain", domain.asString()).toList().isEmpty();
                } catch (Exception e) {
                    if (e.getMessage() == null || !e.getMessage().contains("Class not found")) {
                        throw e;
                    }
                }
                if (exists) {
                    throw new RuntimeException(new DomainListException(domain.name() + " already exists."));
                }
                tx.addV(CLASS_NAME).property(PROP_DOMAIN, domain.asString()).iterate();
            });
        } catch (Exception e) {
            if (e.getCause() instanceof DomainListException) {
                throw (DomainListException) e.getCause();
            }
            throw new DomainListException("Failed to add domain " + domain.name(), e);
        }
    }

    @Override
    protected List<Domain> getDomainListInternal() throws DomainListException {
        try {
            return g.computeInTx(tx -> {
                List<Domain> list = new ArrayList<>();
                try {
                    var results = tx.yql("SELECT domain FROM JamesDomain").toList();
                    for (Object item : results) {
                        if (item instanceof java.util.Map<?, ?> m) {
                            Object d = m.get(PROP_DOMAIN);
                            if (d != null) {
                                list.add(Domain.of(d.toString()));
                            }
                        }
                    }
                } catch (Exception e) {
                    if (e.getMessage() != null && e.getMessage().contains("Class not found")) {
                        return list;
                    }
                    throw e;
                }
                return list;
            });
        } catch (Exception e) {
            throw new DomainListException("Failed to fetch domain list", e);
        }
    }

    @Override
    protected boolean containsDomainInternal(Domain domain) throws DomainListException {
        try {
            return g.computeInTx(tx -> {
                try {
                    var res = tx.yql("SELECT 1 FROM JamesDomain WHERE domain = :domain LIMIT 1", "domain", domain.asString()).toList();
                    return !res.isEmpty();
                } catch (Exception e) {
                    if (e.getMessage() != null && e.getMessage().contains("Class not found")) {
                        return false;
                    }
                    throw e;
                }
            });
        } catch (Exception e) {
            throw new DomainListException("Failed to check if domain exists: " + domain.name(), e);
        }
    }

    @Override
    protected void doRemoveDomain(Domain domain) throws DomainListException {
        try {
            boolean removed = YouTrackDBTransactions.computeStrictTx(g, tx -> {
                var traversal = tx.V().hasLabel(CLASS_NAME).has(PROP_DOMAIN, domain.asString());
                if (traversal.hasNext()) {
                    traversal.next().remove();
                    return true;
                }
                return false;
            });
            if (!removed) {
                throw new DomainListException(domain.name() + " was not found");
            }
        } catch (Exception e) {
            if (e instanceof DomainListException) {
                throw (DomainListException) e;
            }
            throw new DomainListException("Failed to remove domain: " + domain.name(), e);
        }
    }
}
