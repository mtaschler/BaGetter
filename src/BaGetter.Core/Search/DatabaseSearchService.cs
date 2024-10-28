using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;
using BaGetter.Protocol.Models;
using Microsoft.EntityFrameworkCore;

namespace BaGetter.Core;

public class DatabaseSearchService : ISearchService
{
    private readonly IContext _context;
    private readonly IFrameworkCompatibilityService _frameworks;
    private readonly ISearchResponseBuilder _searchBuilder;

    public DatabaseSearchService(IContext context, IFrameworkCompatibilityService frameworks, ISearchResponseBuilder searchBuilder)
    {
        ArgumentNullException.ThrowIfNull(context);
        ArgumentNullException.ThrowIfNull(frameworks);
        ArgumentNullException.ThrowIfNull(searchBuilder);

        _context = context;
        _frameworks = frameworks;
        _searchBuilder = searchBuilder;
    }

    public async Task<SearchResponse> SearchAsync(SearchRequest request, CancellationToken cancellationToken)
    {
        var frameworks = GetCompatibleFrameworksOrNull(request.Framework);

        IQueryable<Package> search = _context.Packages;
        search = ApplySearchQuery(search, request.Query);
        search = ApplySearchFilters(
            search,
            request.IncludePrerelease,
            request.IncludeSemVer2,
            request.PackageType,
            frameworks);

        // We need to apply the search query against only the latest version of a package
        // otherwise we might match older versions that contain out-of-date data.
        IQueryable<Package> latestPackages = _context.Packages;
        latestPackages = ApplySearchFilters(
            latestPackages,
            request.IncludePrerelease,
            request.IncludeSemVer2,
            request.PackageType,
            frameworks);
        latestPackages = latestPackages.GroupBy(p => p.Id)
            .Select(g => g.OrderByDescending(a => a.Key).First());

        var packageIds = search
            .Where(p => latestPackages.Contains(p))
            .Select(p => p.Id)
            .Distinct()
            .OrderBy(id => id)
            .Skip(request.Skip)
            .Take(request.Take);

        // This query MUST fetch all versions for each package that matches the search,
        // otherwise the results for a package's latest version may be incorrect.
        // If possible, we'll find all these packages in a single query by matching
        // the package IDs in a subquery. Otherwise, run two queries:
        //   1. Find the package IDs that match the search
        //   2. Find all package versions for these package IDs
        if (_context.SupportsLimitInSubqueries)
        {
            search = _context.Packages.Where(p => packageIds.Contains(p.Id));
        }
        else
        {
            var packageIdResults = await packageIds.ToListAsync(cancellationToken);

            search = _context.Packages.Where(p => packageIdResults.Contains(p.Id));
        }

        search = ApplySearchFilters(
            search,
            request.IncludePrerelease,
            request.IncludeSemVer2,
            request.PackageType,
            frameworks);

        var results = await search.ToListAsync(cancellationToken);
        var groupedResults = results
            .GroupBy(p => p.Id, StringComparer.OrdinalIgnoreCase)
            .Select(group => new PackageRegistration(group.Key, group.ToList()))
            .ToList();

        return _searchBuilder.BuildSearch(groupedResults);
    }

    public async Task<AutocompleteResponse> AutocompleteAsync(AutocompleteRequest request, CancellationToken cancellationToken)
    {
        IQueryable<Package> search = _context.Packages;

        search = ApplySearchQuery(search, request.Query);
        search = ApplySearchFilters(
            search,
            request.IncludePrerelease,
            request.IncludeSemVer2,
            request.PackageType,
            frameworks: null);

        var packageIds = await search
            .OrderByDescending(p => p.Downloads)
            .Select(p => p.Id)
            .Distinct()
            .Skip(request.Skip)
            .Take(request.Take)
            .ToListAsync(cancellationToken);

        return _searchBuilder.BuildAutocomplete(packageIds);
    }

    public async Task<AutocompleteResponse> ListPackageVersionsAsync(VersionsRequest request, CancellationToken cancellationToken)
    {
        var packageId = request.PackageId.ToLower();
        var search = _context
            .Packages
            .Where(p => p.Id.ToLower().Equals(packageId));

        search = ApplySearchFilters(
            search,
            request.IncludePrerelease,
            request.IncludeSemVer2,
            packageType: null,
            frameworks: null);

        var packageVersions = await search
            .Select(p => p.NormalizedVersionString)
            .ToListAsync(cancellationToken);

        return _searchBuilder.BuildAutocomplete(packageVersions);
    }

    public async Task<DependentsResponse> FindDependentsAsync(string packageId, CancellationToken cancellationToken)
    {
        var dependents = await _context
            .Packages
            .Where(p => p.Listed)
            .OrderByDescending(p => p.Downloads)
            .Where(p => p.Dependencies.Any(d => d.Id == packageId))
            .Take(20)
            .Select(r => new PackageDependent
            {
                Id = r.Id,
                Description = r.Description,
                TotalDownloads = r.Downloads
            })
            .Distinct()
            .ToListAsync(cancellationToken);

        return _searchBuilder.BuildDependents(dependents);
    }

    [System.Diagnostics.CodeAnalysis.SuppressMessage("Performance", "CA1862:Use the 'StringComparison' method overloads to perform case-insensitive string comparisons", Justification = "Not for EF queries")]
    private static IQueryable<Package> ApplySearchQuery(IQueryable<Package> query, string search)
    {
        if (string.IsNullOrEmpty(search))
        {
            return query;
        }

        search = search.ToLowerInvariant();

        var searchTerms = search.Split(' ');
        var idSearchTerms = new List<string>();

        foreach (var searchTerm in searchTerms)
        {
            var colonIndex = searchTerm.IndexOf(':');
            if (colonIndex == -1)
            {
                idSearchTerms.Add(searchTerm);
            }
            else
            {
                var property = searchTerm.Substring(0, colonIndex);
                var term = searchTerm.Substring(colonIndex + 1);

                if (property == "id")
                {
                    idSearchTerms.Add(term);
                }
                else
                {
                    query = ApplySearchTerm(query, property, term);
                }
            }
        }

        if (idSearchTerms.Count != 0)
        {
            query = ApplyIdSearchTerms(query, idSearchTerms);
        }

        return query;
    }

    [System.Diagnostics.CodeAnalysis.SuppressMessage("Performance", "CA1862:Use the 'StringComparison' method overloads to perform case-insensitive string comparisons", Justification = "Not for EF queries")]
    private static IQueryable<Package> ApplySearchTerm(IQueryable<Package> query, string property, string term)
    {
        return property switch
        {
            "packageid" => query.Where(p => p.Id.ToLower() == term),
            "version" => query.Where(p => p.NormalizedVersionString.ToLower() == term),
            "title" => query.Where(p => p.Title.ToLower().Contains(term)),
            "tags" => query.Where(p => ((string)(object)p.Tags).ToLower().Contains(term)),
            "author" => query.Where(p => ((string)(object)p.Authors).ToLower().Contains(term)),
            "description" => query.Where(p => p.Description.ToLower().Contains(term)),
            "summary" => query.Where(p => p.Summary.ToLower().Contains(term)),
            _ => query
        };
    }

    [System.Diagnostics.CodeAnalysis.SuppressMessage("Performance", "CA1862:Use the 'StringComparison' method overloads to perform case-insensitive string comparisons", Justification = "Not for EF queries")]
    private static IQueryable<Package> ApplyIdSearchTerms(IQueryable<Package> query, List<string> idSearchTerms)
    {
        var expressions = idSearchTerms.Select(s => (Expression<Func<Package, bool>>)(p => p.Id.ToLower().Contains(s))).ToList();

        if (expressions.Count == 1)
        {
            return query.Where(expressions[0]);
        }

        var orExpression = expressions.Skip(2).Aggregate(
            Expression.OrElse(expressions[0].Body, Expression.Invoke(expressions[1], expressions[0].Parameters[0])),
            (x, y) => Expression.OrElse(x, Expression.Invoke(y, expressions[0].Parameters[0])));

        return query.Where(Expression.Lambda<Func<Package, bool>>(orExpression, expressions[0].Parameters));
    }

    private static IQueryable<Package> ApplySearchFilters(
        IQueryable<Package> query,
        bool includePrerelease,
        bool includeSemVer2,
        string packageType,
        IReadOnlyList<string> frameworks)
    {
        if (!includePrerelease)
        {
            query = query.Where(p => !p.IsPrerelease);
        }

        if (!includeSemVer2)
        {
            query = query.Where(p => p.SemVerLevel != SemVerLevel.SemVer2);
        }

        if (!string.IsNullOrEmpty(packageType))
        {
            query = query.Where(p => p.PackageTypes.Any(t => t.Name == packageType));
        }

        if (frameworks != null)
        {
            query = query.Where(p => p.TargetFrameworks.Any(f => frameworks.Contains(f.Moniker)));
        }

        return query.Where(p => p.Listed);
    }

    private IReadOnlyList<string> GetCompatibleFrameworksOrNull(string framework)
    {
        if (framework == null) return null;

        return _frameworks.FindAllCompatibleFrameworks(framework);
    }
}
