package org.apache.james.youtrackdb;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import jakarta.inject.Inject;

import org.apache.james.core.Domain;
import org.apache.james.dnsservice.api.DNSService;
import org.apache.james.domainlist.api.DomainListException;
import org.apache.james.domainlist.lib.AbstractDomainList;

import com.jetbrains.youtrackdb.api.exception.RecordDuplicatedException;
import com.jetbrains.youtrackdb.api.gremlin.YTDBGraphTraversalSource;

public class YouTrackDBDomainList extends AbstractDomainList {
    private static final String CLASS_NAME = "JamesDomain";
    private static final String PROP_DOMAIN = "domain";

    private final YTDBGraphTraversalSource g;

    @Inject
    public YouTrackDBDomainList(DNSService dnsService, YTDBGraphTraversalSource g) {
        super(dnsService);
        this.g = g;
    }

    private String canonicalDomain(Domain domain) {
        return java.net.IDN.toASCII(domain.asString(), java.net.IDN.USE_STD3_ASCII_RULES);
    }

    @Override
    public void addDomain(Domain domain) throws DomainListException {
        String canon = canonicalDomain(domain);
        try {
            YouTrackDBTransactions.executeStrictTx(g, tx ->
                tx.addV(CLASS_NAME).property(PROP_DOMAIN, canon).iterate());
        } catch (Exception e) {
            if (YouTrackDBTransactions.hasCause(e, RecordDuplicatedException.class)) {
                throw new DomainListException(domain.name() + " already exists.");
            }
            throw new DomainListException("Failed to add domain " + domain.name(), e);
        }
    }

    @Override
    protected List<Domain> getDomainListInternal() throws DomainListException {
        try {
            List<Map<String, Object>> rows = YouTrackDBTransactions.queryRows(g, "SELECT domain FROM JamesDomain");
            List<Domain> domains = new ArrayList<>(rows.size());
            for (Map<String, Object> row : rows) {
                Object stored = row.get(PROP_DOMAIN);
                if (stored != null) {
                    domains.add(Domain.of(stored.toString()));
                }
            }
            return domains;
        } catch (Exception e) {
            throw new DomainListException("Failed to fetch domain list", e);
        }
    }

    @Override
    protected boolean containsDomainInternal(Domain domain) throws DomainListException {
        String canon = canonicalDomain(domain);
        try {
            return !YouTrackDBTransactions.queryRows(g,
                "SELECT 1 FROM JamesDomain WHERE domain = :domain LIMIT 1", "domain", canon).isEmpty();
        } catch (Exception e) {
            throw new DomainListException("Failed to check if domain exists: " + domain.name(), e);
        }
    }

    @Override
    protected void doRemoveDomain(Domain domain) throws DomainListException {
        if (!containsDomain(domain)) {
            throw new DomainListException(domain.name() + " was not found");
        }
        String canon = canonicalDomain(domain);
        try {
            YouTrackDBTransactions.executeStrictTx(g, tx ->
                tx.command("DELETE VERTEX JamesDomain WHERE domain = :domain", "domain", canon));
        } catch (Exception e) {
            throw new DomainListException("Failed to remove domain: " + domain.name(), e);
        }
    }
}
